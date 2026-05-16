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
 * {@link TransformStage} that returns {@link Math#abs(double)}.
 */
@StageSpec(
    id = "TRANSFORM_ABS_DOUBLE",
    displayName = "Abs double",
    description = "DOUBLE -> DOUBLE",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AbsDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Constructs an abs-double stage.
     *
     * @return the stage
     */
    public static @NotNull AbsDoubleTransform of() {
        return new AbsDoubleTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        return input == null ? null : Math.abs(input);
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
        return "Abs double";
    }

}
