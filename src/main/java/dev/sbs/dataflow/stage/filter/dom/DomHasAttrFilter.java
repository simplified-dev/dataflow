package dev.sbs.dataflow.stage.filter.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.util.List;

/**
 * {@link FilterStage} keeping every DOM element that has the configured attribute.
 * If {@link #expectedValue} is non-null, the attribute value must additionally match.
 */
@StageSpec(
    id = "FILTER_DOM_HAS_ATTR",
    displayName = "Has attribute",
    description = "List<DOM_NODE> -> List<DOM_NODE>",
    category = StageSpec.Category.FILTER_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomHasAttrFilter implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_NODE = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String attributeName;

    private final @Nullable String expectedValue;

    /**
     * Constructs a has-attribute filter that ignores attribute value.
     *
     * @param attributeName the attribute that must be present
     * @return the stage
     */
    public static @NotNull DomHasAttrFilter of(@NotNull String attributeName) {
        return new DomHasAttrFilter(attributeName, null);
    }

    /**
     * Constructs a has-attribute filter that additionally matches the attribute value.
     *
     * @param attributeName the attribute that must be present
     * @param expectedValue the required attribute value
     * @return the stage
     */
    public static @NotNull DomHasAttrFilter of(
        @Configurable(label = "Attribute name", placeholder = "class")
        @NotNull String attributeName,
        @Configurable(label = "Expected value (optional)", placeholder = "primary", optional = true)
        @Nullable String expectedValue
    ) {
        return new DomHasAttrFilter(attributeName, expectedValue);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        return input.stream()
            .filter(el -> el.hasAttr(this.attributeName) &&
                (this.expectedValue == null || this.expectedValue.equals(el.attr(this.attributeName))))
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> inputType() {
        return LIST_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return LIST_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return this.expectedValue == null
            ? "Has attr '" + this.attributeName + "'"
            : "Attr " + this.attributeName + "='" + this.expectedValue + "'";
    }

}
