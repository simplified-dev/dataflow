package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@link JsonElement#getAsInt()}, or {@code null} when
 * the element is not a numeric primitive.
 */
public final class JsonAsIntTransform implements TransformStage<JsonElement, Integer> {

    /**
     * Constructs a json-as-int stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsIntTransform create() {
        return new JsonAsIntTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<JsonElement> inputType() { return DataTypes.JSON_ELEMENT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> outputType()    { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                    { return StageId.TRANSFORM_JSON_AS_INT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                  { return "JSON as int"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull() || !input.isJsonPrimitive()) return null;
        try {
            return input.getAsInt();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

}
