package dev.sbs.dataflow.stage.filter;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * {@link FilterStage} that returns the input list with duplicate elements removed,
 * preserving first-occurrence order. Equality is by {@link Object#equals(Object)}.
 *
 * @param <T> element type
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class FilterDistinct<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;
    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a distinct filter for the given element type.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull FilterDistinct<T> of(@NotNull DataType<T> elementType) {
        return new FilterDistinct<>(elementType, DataType.list(elementType));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.FILTER_DISTINCT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Distinct " + this.elementType.label();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        return List.copyOf(new LinkedHashSet<>(input));
    }

}
