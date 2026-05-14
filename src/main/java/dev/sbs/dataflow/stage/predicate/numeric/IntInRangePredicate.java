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
public final class IntInRangePredicate implements TransformStage<Integer, Boolean> {

    private final int min;

    private final int max;

    /**
     * Constructs an int in-range predicate, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull IntInRangePredicate of(int min, int max) {
        return new IntInRangePredicate(min, max);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull IntInRangePredicate fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getInt("min"), cfg.getInt("max"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .integer("min", this.min)
            .integer("max", this.max)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        return input != null && input >= this.min && input <= this.max;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> inputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_INT_IN_RANGE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Int in [" + this.min + ", " + this.max + "]";
    }

}
