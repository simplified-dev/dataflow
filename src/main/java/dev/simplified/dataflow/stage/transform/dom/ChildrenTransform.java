package dev.simplified.dataflow.stage.transform.dom;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
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
    id = "TRANSFORM_DOM_CHILDREN",
    displayName = "DOM children",
    description = "DOM_NODE -> List<DOM_NODE>",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ChildrenTransform implements TransformStage<Element, List<Element>> {

    private static final @NotNull DataType<List<Element>> OUTPUT = DataType.list(DataTypes.DOM_NODE);

    /**
     * Constructs a children stage.
     *
     * @return the stage
     */
    public static @NotNull ChildrenTransform of() {
        return new ChildrenTransform();
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
    public @NotNull DataType<List<Element>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "DOM children";
    }

}
