package dev.sbs.dataflow.stage.predicate.numeric;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@code true} when the input is strictly greater than the configured threshold.
 */
@StageSpec(
    id = "PREDICATE_DOUBLE_GREATER_THAN",
    displayName = "Double >",
    description = "DOUBLE -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_NUMERIC
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DoubleGreaterThanPredicate implements TransformStage<Double, Boolean> {

    private final double threshold;

    /**
     * Constructs a double greater-than predicate.
     *
     * @param threshold exclusive lower bound
     * @return the stage
     */
    public static @NotNull DoubleGreaterThanPredicate of(
        @Configurable(label = "Threshold", placeholder = "0.0")
        double threshold
    ) {
        return new DoubleGreaterThanPredicate(threshold);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input != null && input > this.threshold;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> inputType() {
        return DataTypes.DOUBLE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Double > " + this.threshold;
    }

}
