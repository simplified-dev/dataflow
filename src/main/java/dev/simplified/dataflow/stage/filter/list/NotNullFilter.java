package dev.simplified.dataflow.stage.filter.list;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.FilterStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * {@link FilterStage} dropping {@code null} elements from the list.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "FILTER_NOT_NULL",
    displayName = "Not null",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.FILTER_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NotNullFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a not-null filter for the given element type.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull NotNullFilter<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType
    ) {
        return new NotNullFilter<>(elementType, DataType.list(elementType));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        return input == null ? null : input.stream()
            .filter(Objects::nonNull)
            .collect(Concurrent.toUnmodifiableList());
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
    public @NotNull String summary() {
        return "Not null " + this.elementType.label();
    }

}
