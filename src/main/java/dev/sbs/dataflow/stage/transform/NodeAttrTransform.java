package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns a named attribute value of a jsoup {@link Element}.
 * Returns the empty string when the attribute is absent, matching jsoup's convention.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class NodeAttrTransform implements TransformStage<Element, String> {

    private final @NotNull String attributeName;

    /**
     * Constructs a node-attribute stage.
     *
     * @param attributeName the attribute to extract
     * @return the stage
     */
    public static @NotNull NodeAttrTransform of(@NotNull String attributeName) {
        return new NodeAttrTransform(attributeName);
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
        return StageId.TRANSFORM_NODE_ATTR;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Attr '" + this.attributeName + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        return input.attr(this.attributeName);
    }

}
