package dev.sbs.dataflow.stage.filter.string;

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

import java.util.List;
import java.util.regex.Pattern;

/**
 * {@link FilterStage} keeping every string whose contents are a regex match.
 */
@StageSpec(
    displayName = "Matches regex",
    description = "List<STRING> -> List<STRING>",
    category = StageSpec.Category.FILTER_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringMatchesFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a string-matches filter.
     *
     * @param regex the pattern that must {@link Pattern#matcher(CharSequence) find} a match in each element
     * @return the stage
     */
    public static @NotNull StringMatchesFilter of(
        @Configurable(label = "Regex", placeholder = "^foo")
        @NotNull String regex
    ) {
        return new StringMatchesFilter(regex, Pattern.compile(regex));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        if (input == null) return null;
        return input.stream()
            .filter(s -> s != null && this.pattern.matcher(s).find())
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> inputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_STRING_MATCHES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> outputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Matches '" + this.regex + "'";
    }

}
