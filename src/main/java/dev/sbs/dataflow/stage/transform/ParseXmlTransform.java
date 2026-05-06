package dev.sbs.dataflow.stage.transform;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.source.XmlBridge;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_XML} body into a Gson
 * {@link JsonElement} via {@link XmlBridge}.
 */
public final class ParseXmlTransform implements TransformStage<String, JsonElement> {

    /**
     * Constructs a parse-XML stage.
     *
     * @return the stage
     */
    public static @NotNull ParseXmlTransform create() {
        return new ParseXmlTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_XML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.PARSE_XML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as XML";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return XmlBridge.parse(input);
    }

}
