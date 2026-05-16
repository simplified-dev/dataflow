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
 * {@link TransformStage} that returns {@code true} when the input is strictly greater than the configured threshold.
 */
@StageSpec(
    id = "PREDICATE_LONG_GREATER_THAN",
    displayName = "Long >",
    description = "LONG -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_NUMERIC
)
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
    public static @NotNull LongGreaterThanPredicate of(
        @Configurable(label = "Threshold", placeholder = "0")
        long threshold
    ) {
        return new LongGreaterThanPredicate(threshold);
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
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long > " + this.threshold;
    }

}
