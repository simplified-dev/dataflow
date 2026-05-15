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
import java.util.regex.Pattern;

/**
 * {@link FilterStage} keeping every DOM element whose text matches the configured regex.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTextMatchesFilter implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_NODE = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a DOM-text-matches filter.
     *
     * @param regex the regex pattern that must {@link Pattern#matcher(CharSequence) find} a match in each element's text
     * @return the stage
     */
    public static @NotNull DomTextMatchesFilter of(@NotNull String regex) {
        return new DomTextMatchesFilter(regex, Pattern.compile(regex));
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull DomTextMatchesFilter fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("regex"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("regex", this.regex)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        return input.stream()
            .filter(el -> this.pattern.matcher(el.text()).find())
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
        return StageKind.FILTER_DOM_TEXT_MATCHES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return LIST_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text matches '" + this.regex + "'";
    }

}
