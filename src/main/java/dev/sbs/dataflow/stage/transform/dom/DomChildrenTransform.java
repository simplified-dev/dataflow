package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.util.List;

/**
 * {@link TransformStage} that returns the direct children of the input {@link Element}.
 */
@StageSpec(
    displayName = "DOM children",
    description = "DOM_NODE -> List<DOM_NODE>",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomChildrenTransform implements TransformStage<Element, List<Element>> {

    private static final @NotNull DataType<List<Element>> OUTPUT = DataType.list(DataTypes.DOM_NODE);

    /**
     * Constructs a children stage.
     *
     * @return the stage
     */
    public static @NotNull DomChildrenTransform of() {
        return new DomChildrenTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : Concurrent.newUnmodifiableList(input.children());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_DOM_CHILDREN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "DOM children";
    }

}
