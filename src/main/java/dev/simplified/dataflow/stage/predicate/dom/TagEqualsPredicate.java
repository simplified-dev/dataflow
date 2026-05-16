package dev.simplified.dataflow.stage.predicate.dom;

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
 * {@link TransformStage} that returns {@code true} when the input element's tag name matches the configured target (case-insensitive).
 */
@StageSpec(
    id = "PREDICATE_DOM_TAG_EQUALS",
    displayName = "Tag equals",
    description = "DOM_NODE -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TagEqualsPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String tagName;

    /**
     * Constructs a tag-equals predicate (case-insensitive).
     *
     * @param tagName the tag name to require
     * @return the stage
     */
    public static @NotNull TagEqualsPredicate of(
        @Configurable(label = "Tag", placeholder = "a")
        @NotNull String tagName
    ) {
        return new TagEqualsPredicate(tagName);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input != null && input.tagName().equalsIgnoreCase(this.tagName);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Tag = '" + this.tagName + "'";
    }

}
