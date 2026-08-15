package dev.simplified.dataflow.stage.transform.dom;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@link Element#parent()}, or {@code null} when the
 * element is at the document root.
 */
@StageSpec(
    id = "TRANSFORM_DOM_PARENT",
    displayName = "DOM parent",
    description = "DOM_NODE -> DOM_NODE",
    category = StageSpec.Category.TRANSFORM_DOM
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParentTransform implements TransformStage<Element, Element> {

    /**
     * Constructs a parent stage.
     *
     * @return the stage
     */
    public static @NotNull ParentTransform of() {
        return new ParentTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Element execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : input.parent();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> outputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "DOM parent";
    }

}
