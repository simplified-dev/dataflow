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

/** {@link FilterStage} keeping doubles strictly greater than the configured threshold. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class DoubleGreaterThanFilter implements FilterStage<Double> {

    private static final @NotNull DataType<List<Double>> LIST_DOUBLE = DataType.list(DataTypes.DOUBLE);
    private final double threshold;

    /**
     * Constructs a double greater-than filter.
     *
     * @param threshold elements must be strictly greater than this
     * @return the stage
     */
    public static @NotNull DoubleGreaterThanFilter of(double threshold) {
        return new DoubleGreaterThanFilter(threshold);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Double>> inputType()  { return LIST_DOUBLE; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Double>> outputType() { return LIST_DOUBLE; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_DOUBLE_GREATER_THAN; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Double > " + this.threshold; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Double> execute(@NotNull PipelineContext ctx, @Nullable List<Double> input) {
        return input == null ? null : input.stream().filter(d -> d != null && d > this.threshold).collect(Collectors.toUnmodifiableList());
    }

}
