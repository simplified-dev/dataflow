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
 * {@link TransformStage} that returns {@link Math#abs(long)}.
 */
@StageSpec(
    id = "TRANSFORM_ABS_LONG",
    displayName = "Abs long",
    description = "LONG -> LONG",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AbsLongTransform implements TransformStage<Long, Long> {

    /**
     * Constructs an abs-long stage.
     *
     * @return the stage
     */
    public static @NotNull AbsLongTransform of() {
        return new AbsLongTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Long execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input == null ? null : Math.abs(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Long> inputType() {
        return DataTypes.LONG;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Long> outputType() {
        return DataTypes.LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Abs long";
    }

}
