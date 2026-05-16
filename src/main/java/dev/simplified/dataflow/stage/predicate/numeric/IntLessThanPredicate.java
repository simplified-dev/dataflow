package dev.simplified.dataflow.stage.predicate.numeric;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@code true} when the input is strictly less than the configured threshold.
 */
@StageSpec(
    id = "PREDICATE_INT_LESS_THAN",
    displayName = "Int <",
    description = "INT -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_NUMERIC
)
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
    public static @NotNull IntLessThanPredicate of(
        @Configurable(label = "Threshold", placeholder = "0")
        int threshold
    ) {
        return new IntLessThanPredicate(threshold);
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
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Int < " + this.threshold;
    }

}
