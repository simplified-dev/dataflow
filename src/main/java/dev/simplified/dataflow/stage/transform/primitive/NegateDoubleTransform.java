package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns the unary negation of a {@link Double}.
 */
@StageSpec(
    id = "TRANSFORM_NEGATE_DOUBLE",
    displayName = "Negate double",
    description = "DOUBLE -> DOUBLE",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NegateDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Constructs a negate-double stage.
     *
     * @return the stage
     */
    public static @NotNull NegateDoubleTransform of() {
        return new NegateDoubleTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input == null ? null : -input;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> inputType() {
        return DataTypes.DOUBLE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Negate double";
    }

}
