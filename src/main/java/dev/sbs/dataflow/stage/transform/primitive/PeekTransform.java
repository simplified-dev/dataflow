package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Identity {@link TransformStage} that logs the input value through
 * {@link PipelineContext#log()} before passing it through unchanged. Mirrors
 * {@link java.util.stream.Stream#peek}.
 *
 * @param <T> value type
 */
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
    public static <T> @NotNull PeekTransform<T> of(@NotNull DataType<T> valueType, @NotNull String label) {
        return new PeekTransform<>(valueType, label);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("valueType", this.valueType)
            .string("label", this.label)
            .build();
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
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_PEEK;
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
