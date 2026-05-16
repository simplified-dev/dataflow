package dev.simplified.dataflow.stage.transform.dom;

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

/**
 * {@link TransformStage} that returns {@link Element#ownText()} - the element's direct
 * text content, excluding text from descendant elements.
 */
@StageSpec(
    id = "TRANSFORM_DOM_OWN_TEXT",
    displayName = "DOM ownText",
    description = "DOM_NODE -> STRING",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OwnTextTransform implements TransformStage<Element, String> {

    /**
     * Constructs an own-text stage.
     *
     * @return the stage
     */
    public static @NotNull OwnTextTransform of() {
        return new OwnTextTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input == null ? null : input.ownText();
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
    public @NotNull String summary() {
        return "DOM ownText";
    }

}
