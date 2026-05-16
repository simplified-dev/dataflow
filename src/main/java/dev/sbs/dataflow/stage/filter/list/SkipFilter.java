package dev.sbs.dataflow.stage.filter.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link FilterStage} dropping the first {@code n} elements of the list.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "FILTER_SKIP",
    displayName = "Skip first N",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.FILTER_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SkipFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final int count;

    /**
     * Constructs a skip-first-n filter.
     *
     * @param elementType element type of the list
     * @param count how many leading elements to drop
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull SkipFilter<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Count", placeholder = "10")
        int count
    ) {
        return new SkipFilter<>(elementType, DataType.list(elementType), Math.max(0, count));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        if (this.count >= input.size()) return Concurrent.newUnmodifiableList();
        return Concurrent.newUnmodifiableList(input.subList(this.count, input.size()));
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
        return "Skip " + this.count;
    }

}
