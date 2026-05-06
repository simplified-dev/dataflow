package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@link Element#outerHtml()} - the element's tag and
 * its children rendered as HTML markup.
 */
public final class DomOuterHtmlTransform implements TransformStage<Element, String> {

    /**
     * Constructs an outer-HTML stage.
     *
     * @return the stage
     */
    public static @NotNull DomOuterHtmlTransform create() {
        return new DomOuterHtmlTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Element> inputType() { return DataTypes.DOM_NODE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_DOM_OUTER_HTML; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "DOM outerHtml"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : input.outerHtml();
    }

}
