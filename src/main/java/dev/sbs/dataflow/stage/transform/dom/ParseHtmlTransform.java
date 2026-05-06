package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_HTML} body into a jsoup
 * {@link Element} suitable for CSS-selector traversal.
 */
public final class ParseHtmlTransform implements TransformStage<String, Element> {

    /**
     * Constructs a parse-HTML stage.
     *
     * @return the stage
     */
    public static @NotNull ParseHtmlTransform create() {
        return new ParseHtmlTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_HTML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> outputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.PARSE_HTML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as HTML";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Element execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return Jsoup.parse(input);
    }

}
