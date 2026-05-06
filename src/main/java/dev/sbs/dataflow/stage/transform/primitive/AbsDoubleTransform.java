package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns {@link Math#abs(double)}. */
public final class AbsDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Constructs an abs-double stage.
     *
     * @return the stage
     */
    public static @NotNull AbsDoubleTransform create() { return new AbsDoubleTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Double> inputType()  { return DataTypes.DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Double> outputType() { return DataTypes.DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_ABS_DOUBLE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Abs double"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input == null ? null : Math.abs(input);
    }

}
