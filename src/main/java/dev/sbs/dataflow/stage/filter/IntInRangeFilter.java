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

/** {@link FilterStage} keeping ints in the inclusive range {@code [min, max]}. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class IntInRangeFilter implements FilterStage<Integer> {

    private static final @NotNull DataType<List<Integer>> LIST_INT = DataType.list(DataTypes.INT);
    private final int min;
    private final int max;

    /**
     * Constructs an int in-range filter, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull IntInRangeFilter of(int min, int max) {
        return new IntInRangeFilter(min, max);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Integer>> inputType()  { return LIST_INT; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Integer>> outputType() { return LIST_INT; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                       { return StageId.FILTER_INT_IN_RANGE; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                     { return "Int in [" + this.min + ", " + this.max + "]"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Integer> execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        return input == null ? null : input.stream().filter(i -> i != null && i >= this.min && i <= this.max).collect(Collectors.toUnmodifiableList());
    }

}
