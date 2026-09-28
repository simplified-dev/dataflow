package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.SourceStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Key handling shared by the stages that match {@link JsonObject} rows on a field: the string
 * form a key compares by, the test that decides whether a cell holds a value, and an index of a
 * pipeline operand's rows that is built at most once per {@link PipelineContext}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RowKeys {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final @NotNull DataType<Map<String, JsonObject>> INDEX =
        new DataType.Basic<>((Class) Map.class, "ROW_INDEX");

    /**
     * Returns the string form a key compares by: a primitive's {@link JsonElement#getAsString()},
     * and the compact JSON of an array or object.
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
     * Wraps a rows operand in a one-stage pipeline whose output indexes those rows by the string
     * form of {@code keyField}, in row order, keeping the first row that carries each key and
     * skipping rows that carry none.
     * <p>
     * Evaluate the result through {@link PipelineContext#evaluateOperand(DataPipeline)}: the
     * index is then built at most once per context, and it reads {@code rows} through the same
     * memo, so the rows are read once however many stages consume them. The index is
     * {@code null} when {@code rows} yields {@code null}. Its rows are the operand's own
     * instances and are never to be mutated.
     *
     * @param rows the operand producing the rows
     * @param keyField the field whose string form keys the index
     * @param kindId the wire id of the owning stage, reported by the index step to a tracer
     * @return the index pipeline
     */
    static @NotNull DataPipeline<Map<String, JsonObject>> index(
        @NotNull DataPipeline<List<JsonObject>> rows,
        @NotNull String keyField,
        @NotNull String kindId
    ) {
        return DataPipeline.unchecked(List.of(new IndexSource(rows, keyField, kindId)), INDEX);
    }

    /**
     * Source step of an index pipeline, reading its rows operand and indexing it by one field.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class IndexSource implements SourceStage<Map<String, JsonObject>> {

        private final @NotNull DataPipeline<List<JsonObject>> rows;

        private final @NotNull String keyField;

        private final @NotNull String kindId;

        /** {@inheritDoc} */
        @Override
        public @Nullable Map<String, JsonObject> execute(@NotNull PipelineContext ctx, @Nullable Void input) {
            List<JsonObject> operand = ctx.evaluateOperand(this.rows);
            if (operand == null) return null;
            Map<String, JsonObject> index = new LinkedHashMap<>();

            for (JsonObject row : operand) {
                if (row == null) continue;
                String key = keyOf(row.get(this.keyField));
                if (key != null) index.putIfAbsent(key, row);
            }

            return Collections.unmodifiableMap(index);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull String kindId() {
            return this.kindId;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull DataType<Map<String, JsonObject>> outputType() {
            return INDEX;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull String summary() {
            return "Index rows on '" + this.keyField + "'";
        }

    }

}
