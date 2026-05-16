package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@link Element#parent()}, or {@code null} when the
 * element is at the document root.
 */
@StageSpec(
    displayName = "DOM parent",
    description = "DOM_NODE -> DOM_NODE",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomParentTransform implements TransformStage<Element, Element> {

    /**
     * Constructs a parent stage.
     *
     * @return the stage
     */
    public static @NotNull DomParentTransform of() {
        return new DomParentTransform();
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
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_DOM_PARENT;
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
