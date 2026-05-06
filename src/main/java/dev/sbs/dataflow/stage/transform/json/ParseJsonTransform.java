package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_JSON} body into a Gson
 * {@link JsonElement}.
 */
public final class ParseJsonTransform implements TransformStage<String, JsonElement> {

    /**
     * Constructs a parse-JSON stage.
     *
     * @return the stage
     */
    public static @NotNull ParseJsonTransform create() {
        return new ParseJsonTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_JSON;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.PARSE_JSON;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as JSON";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return JsonParser.parseString(input);
    }

}
