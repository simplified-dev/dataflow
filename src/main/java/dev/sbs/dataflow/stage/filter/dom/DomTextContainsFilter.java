package dev.sbs.dataflow.stage.filter.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageKind;
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
 * {@link FilterStage} over {@code List<Element>} that keeps every element whose
 * {@link Element#text()} contains the configured needle (case-sensitive).
 */
@StageSpec(
    displayName = "Text contains",
    description = "List<DOM_NODE> -> List<DOM_NODE>",
    category = StageSpec.Category.FILTER_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTextContainsFilter implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_OF_NODES = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String needle;

    /**
     * Constructs a node-text-contains filter.
     *
     * @param needle the substring to look for in each node's text
     * @return the stage
     */
    public static @NotNull DomTextContainsFilter of(
        @Configurable(label = "Text contains", placeholder = "Dmg")
        @NotNull String needle
    ) {
        return new DomTextContainsFilter(needle);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        return input.stream()
            .filter(el -> el.text().contains(this.needle))
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> inputType() {
        return LIST_OF_NODES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_DOM_TEXT_CONTAINS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return LIST_OF_NODES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text contains '" + this.needle + "'";
    }

}
