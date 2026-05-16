package dev.sbs.dataflow.stage.predicate.numeric;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@code true} when the input is in the inclusive range {@code [min, max]}.
 */
@StageSpec(
    displayName = "Double in [min, max]",
    description = "DOUBLE -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_NUMERIC
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DoubleInRangePredicate implements TransformStage<Double, Boolean> {

    private final double min;

    private final double max;

    /**
     * Constructs a double in-range predicate, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull DoubleInRangePredicate of(
        @Configurable(label = "Min (inclusive)", placeholder = "0.0")
        double min,
        @Configurable(label = "Max (inclusive)", placeholder = "100.0")
        double max
    ) {
        return new DoubleInRangePredicate(min, max);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input != null && input >= this.min && input <= this.max;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> inputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_DOUBLE_IN_RANGE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Double in [" + this.min + ", " + this.max + "]";
    }

}
