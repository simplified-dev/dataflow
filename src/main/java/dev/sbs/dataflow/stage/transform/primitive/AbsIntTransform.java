package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns {@link Math#abs(int)}. */
public final class AbsIntTransform implements TransformStage<Integer, Integer> {

    /**
     * Constructs an abs-int stage.
     *
     * @return the stage
     */
    public static @NotNull AbsIntTransform create() { return new AbsIntTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> inputType()  { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> outputType() { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                 { return StageId.TRANSFORM_ABS_INT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()               { return "Abs int"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        return input == null ? null : Math.abs(input);
    }

}
