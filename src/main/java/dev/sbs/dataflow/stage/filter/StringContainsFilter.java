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

import java.util.List;
import java.util.stream.Collectors;

/** {@link FilterStage} keeping every string that contains the configured substring. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class StringContainsFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);
    private final @NotNull String needle;

    /**
     * Constructs a string-contains filter.
     *
     * @param needle the substring to look for
     * @return the stage
     */
    public static @NotNull StringContainsFilter of(@NotNull String needle) {
        return new StringContainsFilter(needle);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> inputType()  { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> outputType() { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_STRING_CONTAINS; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Contains '" + this.needle + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream().filter(s -> s != null && s.contains(this.needle)).collect(Collectors.toUnmodifiableList());
    }

}
