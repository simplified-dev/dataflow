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
 * {@link TransformStage} that returns {@link Math#abs(float)}.
 */
@StageSpec(
    id = "TRANSFORM_ABS_FLOAT",
    displayName = "Abs float",
    description = "FLOAT -> FLOAT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AbsFloatTransform implements TransformStage<Float, Float> {

    /**
     * Constructs an abs-float stage.
     *
     * @return the stage
     */
    public static @NotNull AbsFloatTransform of() {
        return new AbsFloatTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        return input == null ? null : Math.abs(input);
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
        return "Abs float";
    }

}
