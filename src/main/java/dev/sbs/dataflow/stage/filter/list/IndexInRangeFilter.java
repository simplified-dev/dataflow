package dev.sbs.dataflow.stage.filter.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.meta.StageSpec;
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
 * {@link FilterStage} that returns a slice of the input list, half-open
 * {@code [from, to)} like {@link List#subList(int, int)}. Out-of-range indices are clamped.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "FILTER_INDEX_IN_RANGE",
    displayName = "Index in [from, to)",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.FILTER_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexInRangeFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final int fromInclusive;

    private final int toExclusive;

    /**
     * Constructs an index-range filter, clamping {@code from} and {@code to} to the list bounds.
     *
     * @param elementType element type of the list
     * @param fromInclusive zero-based start index (inclusive), clamped to {@code [0, size]}
     * @param toExclusive zero-based end index (exclusive), clamped to {@code [from, size]}
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull IndexInRangeFilter<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "From (inclusive)", placeholder = "0")
        int fromInclusive,
        @Configurable(label = "To (exclusive)", placeholder = "10")
        int toExclusive
    ) {
        return new IndexInRangeFilter<>(elementType, DataType.list(elementType), fromInclusive, toExclusive);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        int from = Math.max(0, Math.min(this.fromInclusive, input.size()));
        int to = Math.max(from, Math.min(this.toExclusive, input.size()));
        return Concurrent.newUnmodifiableList(input.subList(from, to));
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
        return "Index in [" + this.fromInclusive + ", " + this.toExclusive + ")";
    }

}
