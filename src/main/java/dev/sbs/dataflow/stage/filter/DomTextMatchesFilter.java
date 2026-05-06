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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@link FilterStage} keeping every DOM element whose text matches the configured regex. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class DomTextMatchesFilter implements FilterStage<Element> {

    private static final @NotNull DataType<List<Element>> LIST_NODE = DataType.list(DataTypes.DOM_NODE);
    private final @NotNull String regex;

    /**
     * Constructs a DOM-text-matches filter.
     *
     * @param regex the regex pattern that must {@link Pattern#matcher(CharSequence) find} a match in each element's text
     * @return the stage
     */
    public static @NotNull DomTextMatchesFilter of(@NotNull String regex) {
        return new DomTextMatchesFilter(regex);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Element>> inputType()  { return LIST_NODE; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Element>> outputType() { return LIST_NODE; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                       { return StageId.FILTER_DOM_TEXT_MATCHES; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                     { return "Text matches '" + this.regex + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Element> execute(@NotNull PipelineContext ctx, @Nullable List<Element> input) {
        if (input == null) return null;
        Pattern p = Pattern.compile(this.regex);
        return input.stream().filter(el -> p.matcher(el.text()).find()).collect(Collectors.toUnmodifiableList());
    }

}
