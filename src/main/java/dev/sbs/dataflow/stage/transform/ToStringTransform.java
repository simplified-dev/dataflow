package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that converts an arbitrary value to its {@link String}
 * representation via {@link String#valueOf(Object)}.
 *
 * @param <T> input type
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class ToStringTransform<T> implements TransformStage<T, String> {

    private final @NotNull DataType<T> inputType;

    /**
     * Constructs a to-string stage for the given input type.
     *
     * @param inputType the input type
     * @return the stage
     * @param <T> input type
     */
    public static <T> @NotNull ToStringTransform<T> of(@NotNull DataType<T> inputType) {
        return new ToStringTransform<>(inputType);
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_TO_STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "To string (" + this.inputType.label() + ")"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable T input) {
        return input == null ? null : String.valueOf(input);
    }

}
