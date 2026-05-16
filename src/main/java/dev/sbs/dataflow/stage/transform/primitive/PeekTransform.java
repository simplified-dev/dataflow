package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

/**
 * Identity {@link TransformStage} that logs the input value through
 * {@link PipelineContext#log()} before passing it through unchanged. Mirrors
 * {@link Stream#peek(java.util.function.Consumer)}.
 *
 * @param <T> value type
 */
@StageSpec(
    id = "TRANSFORM_PEEK",
    displayName = "Peek (log)",
    description = "T -> T",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PeekTransform<T> implements TransformStage<T, T> {

    private final @NotNull DataType<T> valueType;

    private final @NotNull String label;

    /**
     * Constructs a peek stage with the given label.
     *
     * @param valueType the value type flowing through this stage
     * @param label prefix shown in the log line; useful for distinguishing multiple peeks
     * @return the stage
     * @param <T> value type
     */
    public static <T> @NotNull PeekTransform<T> of(
        @Configurable(label = "Value type", placeholder = "STRING")
        @NotNull DataType<T> valueType,
        @Configurable(label = "Label", placeholder = "stage")
        @NotNull String label
    ) {
        return new PeekTransform<>(valueType, label);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable T input) {
        ctx.log().info("[peek] {}: {}", this.label, input);
        return input;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> inputType() {
        return this.valueType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.valueType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Peek [" + this.label + "]";
    }

}
