package dev.sbs.dataflow.stage.filter.string;

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

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@link FilterStage} keeping every string whose contents are a regex match. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class StringMatchesFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);
    private final @NotNull String regex;

    /**
     * Constructs a string-matches filter.
     *
     * @param regex the pattern that must {@link Pattern#matcher(CharSequence) find} a match in each element
     * @return the stage
     */
    public static @NotNull StringMatchesFilter of(@NotNull String regex) {
        return new StringMatchesFilter(regex);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> inputType()  { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> outputType() { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_STRING_MATCHES; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Matches '" + this.regex + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        if (input == null) return null;
        Pattern p = Pattern.compile(this.regex);
        return input.stream().filter(s -> s != null && p.matcher(s).find()).collect(Collectors.toUnmodifiableList());
    }

}
