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
 * {@link FilterStage} keeping longs strictly greater than the configured threshold.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongGreaterThanFilter implements FilterStage<Long> {

    private static final @NotNull DataType<List<Long>> LIST_LONG = DataType.list(DataTypes.LONG);

    private final long threshold;

    /**
     * Constructs a long greater-than filter.
     *
     * @param threshold elements must be strictly greater than this
     * @return the stage
     */
    public static @NotNull LongGreaterThanFilter of(long threshold) {
        return new LongGreaterThanFilter(threshold);
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LongGreaterThanFilter fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getLong("threshold"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .longVal("threshold", this.threshold)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Long> execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        return input == null ? null : input.stream()
            .filter(l -> l != null && l > this.threshold)
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
        return StageKind.FILTER_LONG_GREATER_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> outputType() {
        return LIST_LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long > " + this.threshold;
    }

}
