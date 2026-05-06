package dev.sbs.dataflow.stage.filter;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link FilterStage} over {@code List<Element>} that keeps every element whose
 * {@link Element#text()} contains the configured needle (case-sensitive).
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class FilterDomTextContains implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_OF_NODES = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String needle;

    /**
     * Constructs a node-text-contains filter.
     *
     * @param needle the substring to look for in each node's text
     * @return the stage
     */
    public static @NotNull FilterDomTextContains of(@NotNull String needle) {
        return new FilterDomTextContains(needle);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> inputType() {
        return LIST_OF_NODES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return LIST_OF_NODES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.FILTER_DOM_TEXT_CONTAINS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text contains '" + this.needle + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        return input.stream()
            .filter(el -> el.text().contains(this.needle))
            .collect(Collectors.toUnmodifiableList());
    }

}
