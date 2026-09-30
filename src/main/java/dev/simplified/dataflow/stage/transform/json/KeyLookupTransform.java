package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * {@link TransformStage} that resolves a key against the rows of a second document, read
 * through a pipeline operand, and returns one field of the row it names.
 * <p>
 * The input matches the row whose {@code keyField} has it as its string form - a primitive's
 * text, the compact JSON of an array or object - and when two rows carry one key, the first is
 * the one matched. The result is a copy of that row's {@code valueField}. The table is read and
 * indexed at most once per context, so a lookup inside a Map body over many elements reads its
 * document once; a tracer sees the table's own stages run once and no step for the indexing.
 * <p>
 * A {@code null} input, a {@code null} operand output, a key no row carries, and a row whose
 * {@code valueField} is absent or JSON null all yield {@code null}, so a Map body drops the
 * element and an ObjectBuild output omits the field.
 */
@StageSpec(
    id = "TRANSFORM_KEY_LOOKUP",
    displayName = "Key lookup",
    description = "STRING -> JSON_ELEMENT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class KeyLookupTransform implements TransformStage<String, JsonElement> {

    private static final @NotNull DataType<List<JsonObject>> ROWS = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull String keyField;

    private final @NotNull String valueField;

    private final @NotNull DataPipeline<List<JsonObject>> table;

    /**
     * The table's rows indexed by {@link #keyField}, built at most once per context.
     */
    @Getter(AccessLevel.NONE)
    private final @NotNull RowKeys.Index index;

    /**
     * Constructs a lookup of each input key in the rows {@code table} produces.
     *
     * @param keyField the field of a table row that keys it
     * @param valueField the field of the matched row to return
     * @param table the operand pipeline, whose output must be {@code List<JSON_OBJECT>}
     * @return the stage
     * @throws IllegalArgumentException when {@code table} is invalid or does not produce
     *         {@code List<JSON_OBJECT>}
     */
    @SuppressWarnings("unchecked")
    public static @NotNull KeyLookupTransform of(
        @Configurable(label = "Key field", placeholder = "name")
        @NotNull String keyField,
        @Configurable(label = "Value field", placeholder = "id")
        @NotNull String valueField,
        @Configurable(label = "Table pipeline")
        @NotNull DataPipeline<?> table
    ) {
        ValidationReport report = table.validate(ROWS);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid KeyLookupTransform operand: " + report.issues());

        DataPipeline<List<JsonObject>> rows = (DataPipeline<List<JsonObject>>) table;
        return new KeyLookupTransform(keyField, valueField, rows, RowKeys.index(rows, keyField));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        Map<String, JsonObject> rows = this.index.read(ctx);
        if (rows == null) return null;
        JsonObject row = rows.get(input);
        if (row == null) return null;
        JsonElement value = row.get(this.valueField);
        return value == null || value.isJsonNull() ? null : value.deepCopy();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Key lookup '" + this.keyField + "' -> '" + this.valueField + "'";
    }

}
