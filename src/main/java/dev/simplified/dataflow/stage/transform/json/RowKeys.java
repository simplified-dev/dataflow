package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.transform.list.GroupByTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Key handling shared by the stages that match {@link JsonObject} rows on a field: the string
 * form a key compares by, the test that decides whether a cell holds a value, and an index of a
 * pipeline operand's rows that is built at most once per {@link PipelineContext}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RowKeys {

    /**
     * Returns the string form a key compares by: a primitive's {@link JsonElement#getAsString()},
     * and the compact JSON of an array or object.
     * <p>
     * Two keys are equal when their string forms are, so the number {@code 1} and the text
     * {@code "1"} are one key, {@code 1}, {@code 1.0} and {@code 1e0} are three, and two objects
     * are one key only when they list the same members in the same order. A
     * {@link GroupByTransform} compares its keys by JSON value instead.
     *
     * @param value the key value
     * @return the string form, or {@code null} when {@code value} is absent or JSON null
     */
    static @Nullable String keyOf(@Nullable JsonElement value) {
        if (value == null || value.isJsonNull()) return null;
        if (value.isJsonPrimitive()) return value.getAsString();
        return PipelineGson.gson().toJson(value);
    }

    /**
     * Tests whether a cell holds a value: anything but absent, JSON null, {@code ""}, {@code []}
     * and {@code {}}.
     *
     * @param value the cell value
     * @return {@code true} when the cell holds a value
     */
    static boolean populated(@Nullable JsonElement value) {
        if (value == null || value.isJsonNull()) return false;
        if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) return !value.getAsString().isEmpty();
        if (value.isJsonArray()) return !value.getAsJsonArray().isEmpty();
        if (value.isJsonObject()) return !value.getAsJsonObject().isEmpty();
        return true;
    }

    /**
     * Returns an index of the rows {@code rows} produces by the string form of {@code keyField}.
     *
     * @param rows the operand producing the rows
     * @param keyField the field whose string form keys the index
     * @return the index
     */
    static @NotNull Index index(@NotNull DataPipeline<List<JsonObject>> rows, @NotNull String keyField) {
        return new Index(rows, keyField);
    }

    /**
     * Index of the rows a pipeline operand produces by the string form of one field, built at most
     * once per {@link PipelineContext}.
     * <p>
     * It keeps, in row order, the first row that carries each key and skips rows that carry none.
     * The operand is read through {@link PipelineContext#evaluateOperand(DataPipeline)}, so it runs
     * at most once per context however many stages read it. The index is held by the context
     * through {@link PipelineContext#derive(Object, Supplier)}, keyed by this instance, so it is
     * built at most once per context and released with the context, and this instance holds no
     * rows. The indexing is no stage: a tracer sees the operand's own stages and nothing for the
     * index. The index holds the operand's own row instances, which are never to be mutated.
     */
    static final class Index {

        private final @NotNull DataPipeline<List<JsonObject>> rows;

        private final @NotNull String keyField;

        private Index(@NotNull DataPipeline<List<JsonObject>> rows, @NotNull String keyField) {
            this.rows = rows;
            this.keyField = keyField;
        }

        /**
         * Returns the index of the rows the operand produces in {@code ctx}, built on the first call
         * for that context and held by it from then on.
         *
         * @param ctx the context the operand is read in
         * @return the index, or {@code null} when the operand yields {@code null}
         */
        @Nullable Map<String, JsonObject> read(@NotNull PipelineContext ctx) {
            List<JsonObject> operand = ctx.evaluateOperand(this.rows);
            if (operand == null) return null;
            return ctx.derive(this, () -> this.build(operand));
        }

        private @NotNull Map<String, JsonObject> build(@NotNull List<JsonObject> operand) {
            Map<String, JsonObject> index = new LinkedHashMap<>();

            for (JsonObject row : operand) {
                if (row == null) continue;
                String key = keyOf(row.get(this.keyField));
                if (key != null) index.putIfAbsent(key, row);
            }

            return Collections.unmodifiableMap(index);
        }

    }

}
