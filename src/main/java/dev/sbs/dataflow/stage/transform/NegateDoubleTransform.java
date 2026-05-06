package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns the unary negation of a {@link Double}. */
public final class NegateDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Constructs a negate-double stage.
     *
     * @return the stage
     */
    public static @NotNull NegateDoubleTransform create() { return new NegateDoubleTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Double> inputType()  { return DataTypes.DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Double> outputType() { return DataTypes.DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_NEGATE_DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Negate double"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input == null ? null : -input;
    }

}
