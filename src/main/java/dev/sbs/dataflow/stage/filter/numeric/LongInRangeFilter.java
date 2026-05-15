package dev.sbs.dataflow.stage.filter.numeric;

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

import java.util.List;

/**
 * {@link FilterStage} keeping longs in the inclusive range {@code [min, max]}.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongInRangeFilter implements FilterStage<Long> {

    private static final @NotNull DataType<List<Long>> LIST_LONG = DataType.list(DataTypes.LONG);

    private final long min;

    private final long max;

    /**
     * Constructs a long in-range filter, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull LongInRangeFilter of(long min, long max) {
        return new LongInRangeFilter(min, max);
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LongInRangeFilter fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getLong("min"), cfg.getLong("max"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .longVal("min", this.min)
            .longVal("max", this.max)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Long> execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        return input == null ? null : input.stream()
            .filter(l -> l != null && l >= this.min && l <= this.max)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> inputType() {
        return LIST_LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_LONG_IN_RANGE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> outputType() {
        return LIST_LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long in [" + this.min + ", " + this.max + "]";
    }

}
