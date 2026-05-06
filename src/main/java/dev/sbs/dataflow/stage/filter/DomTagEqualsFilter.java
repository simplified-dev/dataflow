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

/** {@link FilterStage} keeping only elements whose tag name equals the configured target. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
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

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Element>> inputType()  { return LIST_NODE; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Element>> outputType() { return LIST_NODE; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                       { return StageId.FILTER_DOM_TAG_EQUALS; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                     { return "Tag = '" + this.tagName + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        String target = this.tagName.toLowerCase();
        return input.stream().filter(el -> el.tagName().equalsIgnoreCase(target)).collect(Collectors.toUnmodifiableList());
    }

}
