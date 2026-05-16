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
 * {@link TransformStage} that returns the unary negation of a {@link Long}.
 */
@StageSpec(
    id = "TRANSFORM_NEGATE_LONG",
    displayName = "Negate long",
    description = "LONG -> LONG",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NegateLongTransform implements TransformStage<Long, Long> {

    /**
     * Constructs a negate-long stage.
     *
     * @return the stage
     */
    public static @NotNull NegateLongTransform of() {
        return new NegateLongTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Long execute(@NotNull PipelineContext ctx, @Nullable Long input) {
        return input == null ? null : -input;
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
        return "Negate long";
    }

}
