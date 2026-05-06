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
 * {@link TransformStage} that returns the visible text content of a jsoup {@link Element}.
 */
public final class NodeTextTransform implements TransformStage<Element, String> {

    /**
     * Constructs a node-text stage.
     *
     * @return the stage
     */
    public static @NotNull NodeTextTransform create() {
        return new NodeTextTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_NODE_TEXT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Node text";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        return input.text();
    }

}
