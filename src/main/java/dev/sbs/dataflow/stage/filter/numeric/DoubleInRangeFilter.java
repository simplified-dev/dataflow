package dev.sbs.dataflow.stage.filter.numeric;

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

/** {@link FilterStage} keeping doubles in the inclusive range {@code [min, max]}. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class DoubleInRangeFilter implements FilterStage<Double> {

    private static final @NotNull DataType<List<Double>> LIST_DOUBLE = DataType.list(DataTypes.DOUBLE);
    private final double min;
    private final double max;

    /**
     * Constructs a double in-range filter, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull DoubleInRangeFilter of(double min, double max) {
        return new DoubleInRangeFilter(min, max);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Double>> inputType()  { return LIST_DOUBLE; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Double>> outputType() { return LIST_DOUBLE; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_DOUBLE_IN_RANGE; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Double in [" + this.min + ", " + this.max + "]"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Double> execute(@NotNull PipelineContext ctx, @Nullable List<Double> input) {
        return input == null ? null : input.stream().filter(d -> d != null && d >= this.min && d <= this.max).collect(Collectors.toUnmodifiableList());
    }

}
