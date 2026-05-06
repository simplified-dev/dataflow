package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns {@link Math#abs(float)}. */
public final class AbsFloatTransform implements TransformStage<Float, Float> {

    /**
     * Constructs an abs-float stage.
     *
     * @return the stage
     */
    public static @NotNull AbsFloatTransform create() { return new AbsFloatTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Float> inputType()  { return DataTypes.FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Float> outputType() { return DataTypes.FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()               { return StageId.TRANSFORM_ABS_FLOAT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()             { return "Abs float"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        return input == null ? null : Math.abs(input);
    }

}
