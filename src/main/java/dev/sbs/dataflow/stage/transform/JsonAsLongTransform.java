package dev.sbs.dataflow.stage.transform;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@link JsonElement#getAsLong()}, or {@code null}
 * when the element is not a numeric primitive.
 */
public final class JsonAsLongTransform implements TransformStage<JsonElement, Long> {

    /**
     * Constructs a json-as-long stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsLongTransform create() {
        return new JsonAsLongTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<JsonElement> inputType() { return DataTypes.JSON_ELEMENT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Long> outputType()       { return DataTypes.LONG; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                    { return StageId.TRANSFORM_JSON_AS_LONG; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                  { return "JSON as long"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Long execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull() || !input.isJsonPrimitive()) return null;
        try {
            return input.getAsLong();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

}
