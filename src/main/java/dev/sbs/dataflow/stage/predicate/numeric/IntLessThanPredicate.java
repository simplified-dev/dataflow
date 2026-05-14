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

/** {@link TransformStage} that returns {@code true} when the input is strictly less than the configured threshold. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IntLessThanPredicate implements TransformStage<Integer, Boolean> {

    private final int threshold;

    /**
     * Constructs an int less-than predicate.
     *
     * @param threshold exclusive upper bound
     * @return the stage
     */
    public static @NotNull IntLessThanPredicate of(int threshold) {
        return new IntLessThanPredicate(threshold);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull IntLessThanPredicate fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        return input != null && input < this.threshold;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> inputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_INT_LESS_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Int < " + this.threshold;
    }

}
