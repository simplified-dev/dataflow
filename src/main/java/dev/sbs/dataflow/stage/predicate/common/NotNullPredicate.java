package dev.sbs.dataflow.stage.predicate.common;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
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

/**
 * {@link TransformStage} that returns {@code true} when the input is non-{@code null},
 * {@code false} otherwise. Generic over the value type.
 *
 * @param <T> value type
 */
@StageSpec(
    id = "PREDICATE_NOT_NULL",
    displayName = "Not null",
    description = "T -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_COMMON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NotNullPredicate<T> implements TransformStage<T, Boolean> {

    private final @NotNull DataType<T> elementType;

    /**
     * Constructs a not-null predicate for the given value type.
     *
     * @param elementType the value type flowing through this stage
     * @return the stage
     * @param <T> value type
     */
    public static <T> @NotNull NotNullPredicate<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType
    ) {
        return new NotNullPredicate<>(elementType);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Boolean execute(@NotNull PipelineContext ctx, @Nullable T input) {
        return input != null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> inputType() {
        return this.elementType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Not null (" + this.elementType.label() + ")";
    }

}
