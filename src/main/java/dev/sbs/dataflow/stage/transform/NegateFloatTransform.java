package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns the unary negation of a {@link Float}. */
public final class NegateFloatTransform implements TransformStage<Float, Float> {

    /**
     * Constructs a negate-float stage.
     *
     * @return the stage
     */
    public static @NotNull NegateFloatTransform create() { return new NegateFloatTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Float> inputType()  { return DataTypes.FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Float> outputType() { return DataTypes.FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()               { return StageId.TRANSFORM_NEGATE_FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()             { return "Negate float"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        return input == null ? null : -input;
    }

}
