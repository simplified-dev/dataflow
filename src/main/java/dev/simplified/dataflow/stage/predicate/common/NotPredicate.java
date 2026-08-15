package dev.simplified.dataflow.stage.predicate.common;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that inverts an input {@link Boolean}. {@code null} input passes through as {@code null}.
 */
@StageSpec(
    id = "PREDICATE_NOT",
    displayName = "Not",
    description = "BOOLEAN -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_COMMON
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NotPredicate implements TransformStage<Boolean, Boolean> {

    /**
     * Constructs a not predicate.
     *
     * @return the stage
     */
    public static @NotNull NotPredicate of() {
        return new NotPredicate();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Boolean input) {
        return input == null ? null : !input;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> inputType() {
        return DataTypes.BOOLEAN;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Not";
    }

}
