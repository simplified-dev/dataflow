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

/** {@link FilterStage} keeping only strings exactly equal to the configured target. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class StringEqualsFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);
    private final @NotNull String target;

    /**
     * Constructs an equals filter.
     *
     * @param target the exact string to match
     * @return the stage
     */
    public static @NotNull StringEqualsFilter of(@NotNull String target) {
        return new StringEqualsFilter(target);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> inputType()  { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> outputType() { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_STRING_EQUALS; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Equals '" + this.target + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream().filter(this.target::equals).collect(Collectors.toUnmodifiableList());
    }

}
