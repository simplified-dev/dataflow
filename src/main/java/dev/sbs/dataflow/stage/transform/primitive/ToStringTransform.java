package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
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
@StageSpec(
    displayName = "To string",
    description = "T -> STRING",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ToStringTransform<T> implements TransformStage<T, String> {

    private final @NotNull DataType<T> inputType;

    /**
     * Constructs a to-string stage for the given input type.
     *
     * @param inputType the input type
     * @return the stage
     * @param <T> input type
     */
    public static <T> @NotNull ToStringTransform<T> of(
        @Configurable(label = "Input type", placeholder = "INT")
        @NotNull DataType<T> inputType
    ) {
        return new ToStringTransform<>(inputType);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable T input) {
        return input == null ? null : String.valueOf(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_TO_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "To string (" + this.inputType.label() + ")";
    }

}
