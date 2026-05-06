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
 * {@link TransformStage} that returns {@link JsonElement#getAsBoolean()}, or {@code null}
 * when the element is not a primitive.
 */
public final class JsonAsBooleanTransform implements TransformStage<JsonElement, Boolean> {

    /**
     * Constructs a json-as-boolean stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsBooleanTransform create() {
        return new JsonAsBooleanTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<JsonElement> inputType() { return DataTypes.JSON_ELEMENT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Boolean> outputType()    { return DataTypes.BOOLEAN; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                    { return StageId.TRANSFORM_JSON_AS_BOOLEAN; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                  { return "JSON as boolean"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull() || !input.isJsonPrimitive()) return null;
        return input.getAsBoolean();
    }

}
