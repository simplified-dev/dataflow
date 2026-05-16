package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns the unary negation of a {@link Float}.
 */
@StageSpec(
    id = "TRANSFORM_NEGATE_FLOAT",
    displayName = "Negate float",
    description = "FLOAT -> FLOAT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NegateFloatTransform implements TransformStage<Float, Float> {

    /**
     * Constructs a negate-float stage.
     *
     * @return the stage
     */
    public static @NotNull NegateFloatTransform of() {
        return new NegateFloatTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        return input == null ? null : -input;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Float> inputType() {
        return DataTypes.FLOAT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Float> outputType() {
        return DataTypes.FLOAT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Negate float";
    }

}
