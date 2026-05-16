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
 * {@link TransformStage} that returns {@code true} when the input is strictly less than the configured threshold.
 */
@StageSpec(
    id = "PREDICATE_LONG_LESS_THAN",
    displayName = "Long <",
    description = "LONG -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_NUMERIC
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongLessThanPredicate implements TransformStage<Long, Boolean> {

    private final long threshold;

    /**
     * Constructs a long less-than predicate.
     *
     * @param threshold exclusive upper bound
     * @return the stage
     */
    public static @NotNull LongLessThanPredicate of(
        @Configurable(label = "Threshold", placeholder = "0")
        long threshold
    ) {
        return new LongLessThanPredicate(threshold);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input != null && input < this.threshold;
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
        return "Long < " + this.threshold;
    }

}
