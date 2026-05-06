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
 * {@link TransformStage} that returns {@link Element#ownText()} - the element's direct
 * text content, excluding text from descendant elements.
 */
public final class DomOwnTextTransform implements TransformStage<Element, String> {

    /**
     * Constructs an own-text stage.
     *
     * @return the stage
     */
    public static @NotNull DomOwnTextTransform create() {
        return new DomOwnTextTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Element> inputType() { return DataTypes.DOM_NODE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_DOM_OWN_TEXT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "DOM ownText"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : input.ownText();
    }

}
