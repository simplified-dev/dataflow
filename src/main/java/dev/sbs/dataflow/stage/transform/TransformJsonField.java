package dev.sbs.dataflow.stage.transform;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns a single named field of a {@link JsonObject}.
 * Returns {@code null} when the field is absent.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class TransformJsonField implements TransformStage<JsonObject, JsonElement> {

    private final @NotNull String fieldName;

    /**
     * Constructs a JSON-field stage.
     *
     * @param fieldName the field to extract
     * @return the stage
     */
    public static @NotNull TransformJsonField of(@NotNull String fieldName) {
        return new TransformJsonField(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> inputType() {
        return DataTypes.JSON_OBJECT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_JSON_FIELD;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Field '" + this.fieldName + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable JsonObject input) {
        if (input == null) return null;
        return input.get(this.fieldName);
    }

}
