package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@link Element#parent()}, or {@code null} when the
 * element is at the document root.
 */
public final class DomParentTransform implements TransformStage<Element, Element> {

    /**
     * Constructs a parent stage.
     *
     * @return the stage
     */
    public static @NotNull DomParentTransform create() {
        return new DomParentTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Element> inputType()  { return DataTypes.DOM_NODE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Element> outputType() { return DataTypes.DOM_NODE; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                 { return StageId.TRANSFORM_DOM_PARENT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()               { return "DOM parent"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Element execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : input.parent();
    }

}
