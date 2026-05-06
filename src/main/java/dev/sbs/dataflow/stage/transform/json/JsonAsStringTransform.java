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
 * {@link TransformStage} that returns {@link JsonElement#getAsString()}, or {@code null}
 * when the element is not a primitive.
 */
public final class JsonAsStringTransform implements TransformStage<JsonElement, String> {

    /**
     * Constructs a json-as-string stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsStringTransform create() {
        return new JsonAsStringTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<JsonElement> inputType() { return DataTypes.JSON_ELEMENT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType()     { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                    { return StageId.TRANSFORM_JSON_AS_STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                  { return "JSON as string"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull()) return null;
        if (!input.isJsonPrimitive()) return null;
        return input.getAsString();
    }

}
