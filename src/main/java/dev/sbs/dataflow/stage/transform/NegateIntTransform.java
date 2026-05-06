package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns the unary negation of an {@link Integer}. */
public final class NegateIntTransform implements TransformStage<Integer, Integer> {

    /**
     * Constructs a negate-int stage.
     *
     * @return the stage
     */
    public static @NotNull NegateIntTransform create() { return new NegateIntTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> inputType()  { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> outputType() { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                 { return StageId.TRANSFORM_NEGATE_INT; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()               { return "Negate int"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        return input == null ? null : -input;
    }

}
