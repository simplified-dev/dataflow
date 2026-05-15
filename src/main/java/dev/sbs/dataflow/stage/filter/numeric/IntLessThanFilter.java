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
 * {@link FilterStage} keeping ints strictly less than the configured threshold.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IntLessThanFilter implements FilterStage<Integer> {

    private static final @NotNull DataType<List<Integer>> LIST_INT = DataType.list(DataTypes.INT);

    private final int threshold;

    /**
     * Constructs an int less-than filter.
     *
     * @param threshold strict upper bound; elements must be strictly less than this
     * @return the stage
     */
    public static @NotNull IntLessThanFilter of(int threshold) {
        return new IntLessThanFilter(threshold);
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull IntLessThanFilter fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getInt("threshold"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .integer("threshold", this.threshold)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Integer> execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        return input == null ? null : input.stream()
            .filter(i -> i != null && i < this.threshold)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> inputType() {
        return LIST_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_INT_LESS_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> outputType() {
        return LIST_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Int < " + this.threshold;
    }

}
