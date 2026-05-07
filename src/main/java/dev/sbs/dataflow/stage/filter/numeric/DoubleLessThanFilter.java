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

/** {@link FilterStage} keeping doubles strictly less than the configured threshold. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DoubleLessThanFilter implements FilterStage<Double> {

    private static final @NotNull DataType<List<Double>> LIST_DOUBLE = DataType.list(DataTypes.DOUBLE);

    private final double threshold;

    /**
     * Constructs a double less-than filter.
     *
     * @param threshold elements must be strictly less than this
     * @return the stage
     */
    public static @NotNull DoubleLessThanFilter of(double threshold) {
        return new DoubleLessThanFilter(threshold);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .doubleVal("threshold", this.threshold)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Double> execute(@NotNull PipelineContext ctx, @Nullable List<Double> input) {
        return input == null ? null : input.stream()
            .filter(d -> d != null && d < this.threshold)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Double>> inputType() {
        return LIST_DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_DOUBLE_LESS_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Double>> outputType() {
        return LIST_DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Double < " + this.threshold;
    }

}
