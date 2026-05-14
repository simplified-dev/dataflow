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

/** {@link TransformStage} that returns {@code true} when the input is strictly greater than the configured threshold. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongGreaterThanPredicate implements TransformStage<Long, Boolean> {

    private final long threshold;

    /**
     * Constructs a long greater-than predicate.
     *
     * @param threshold exclusive lower bound
     * @return the stage
     */
    public static @NotNull LongGreaterThanPredicate of(long threshold) {
        return new LongGreaterThanPredicate(threshold);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LongGreaterThanPredicate fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input != null && input > this.threshold;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Long> inputType() {
        return DataTypes.LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_LONG_GREATER_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long > " + this.threshold;
    }

}
