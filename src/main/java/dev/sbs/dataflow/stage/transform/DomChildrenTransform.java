package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.util.List;

/**
 * {@link TransformStage} that returns the direct children of the input {@link Element}.
 */
public final class DomChildrenTransform implements TransformStage<Element, List<Element>> {

    private static final @NotNull DataType<List<Element>> OUTPUT = DataType.list(DataTypes.DOM_NODE);

    /**
     * Constructs a children stage.
     *
     * @return the stage
     */
    public static @NotNull DomChildrenTransform create() {
        return new DomChildrenTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Element> inputType()        { return DataTypes.DOM_NODE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<List<Element>> outputType() { return OUTPUT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                       { return StageId.TRANSFORM_DOM_CHILDREN; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()                     { return "DOM children"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Element> execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : List.copyOf(input.children());
    }

}
