package dev.simplified.dataflow.stage.transform.dom;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
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
@StageSpec(
    id = "TRANSFORM_DOM_ATTR",
    displayName = "DOM attribute",
    description = "DOM_NODE -> STRING",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AttrTransform implements TransformStage<Element, String> {

    private final @NotNull String attributeName;

    /**
     * Constructs a DOM attribute stage.
     *
     * @param attributeName the attribute to extract
     * @return the stage
     */
    public static @NotNull AttrTransform of(
        @Configurable(label = "Attribute name", placeholder = "href")
        @NotNull String attributeName
    ) {
        return new AttrTransform(attributeName);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        return input.attr(this.attributeName);
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
        return "Attr '" + this.attributeName + "'";
    }

}
