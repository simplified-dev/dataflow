package dev.sbs.dataflow.stage.filter.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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
 * {@link FilterStage} keeping only elements whose tag name equals the configured target.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTagEqualsFilter implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_NODE = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String tagName;

    /**
     * Constructs a tag-equals filter (case-insensitive).
     *
     * @param tagName the tag name to require
     * @return the stage
     */
    public static @NotNull DomTagEqualsFilter of(@NotNull String tagName) {
        return new DomTagEqualsFilter(tagName);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("tagName", this.tagName)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        String target = this.tagName.toLowerCase();
        return input.stream()
            .filter(el -> el.tagName().equalsIgnoreCase(target))
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> inputType() {
        return LIST_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_DOM_TAG_EQUALS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return LIST_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Tag = '" + this.tagName + "'";
    }

}
