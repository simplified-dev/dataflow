package dev.sbs.dataflow.stage.predicate.numeric;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns {@code true} when the input is in the inclusive range {@code [min, max]}. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongInRangePredicate implements TransformStage<Long, Boolean> {

    private final long min;

    private final long max;

    /**
     * Constructs a long in-range predicate, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull LongInRangePredicate of(long min, long max) {
        return new LongInRangePredicate(min, max);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LongInRangePredicate fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input != null && input >= this.min && input <= this.max;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Long> inputType() {
        return DataTypes.LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_LONG_IN_RANGE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long in [" + this.min + ", " + this.max + "]";
    }

}
