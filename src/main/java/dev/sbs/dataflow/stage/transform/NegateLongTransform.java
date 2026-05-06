package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns the unary negation of a {@link Long}. */
public final class NegateLongTransform implements TransformStage<Long, Long> {

    /**
     * Constructs a negate-long stage.
     *
     * @return the stage
     */
    public static @NotNull NegateLongTransform create() { return new NegateLongTransform(); }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<Long> inputType()  { return DataTypes.LONG; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Long> outputType() { return DataTypes.LONG; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()              { return StageId.TRANSFORM_NEGATE_LONG; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()            { return "Negate long"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Long execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input == null ? null : -input;
    }

}
