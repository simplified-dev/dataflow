package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that serialises a {@link JsonElement} back into its JSON string
 * representation.
 */
public final class JsonStringifyTransform implements TransformStage<JsonElement, String> {

    private static final @NotNull Gson GSON = new Gson();

    /**
     * Constructs a json-stringify stage.
     *
     * @return the stage
     */
    public static @NotNull JsonStringifyTransform create() {
        return new JsonStringifyTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<JsonElement> inputType() { return DataTypes.JSON_ELEMENT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType()     { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                    { return StageId.TRANSFORM_JSON_STRINGIFY; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                  { return "JSON stringify"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        return input == null ? null : GSON.toJson(input);
    }

}
