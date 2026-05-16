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
 * {@link TransformStage} that returns {@link Math#abs(int)}.
 */
@StageSpec(
    id = "TRANSFORM_ABS_INT",
    displayName = "Abs int",
    description = "INT -> INT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AbsIntTransform implements TransformStage<Integer, Integer> {

    /**
     * Constructs an abs-int stage.
     *
     * @return the stage
     */
    public static @NotNull AbsIntTransform of() {
        return new AbsIntTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        return input == null ? null : Math.abs(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> inputType() {
        return DataTypes.INT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> outputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Abs int";
    }

}
