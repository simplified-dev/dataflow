package dev.simplified.dataflow.stage.terminal.collect;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.CollectStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns a contiguous sub-range of the input list over the
 * half-open window {@code [from, to)}. A {@code null} {@code to} means "through the end
 * of the list".
 *
 * @param <T> element type
 */
@StageSpec(
    id = "COLLECT_SUB_LIST",
    displayName = "SubList",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SubListCollect<T> implements CollectStage<List<T>, List<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final int from;

    private final @Nullable Integer to;

    /**
     * Constructs a sub-list collect stage over the half-open window {@code [from, to)}.
     *
     * @param elementType element type of the list
     * @param from inclusive start index; clamped to {@code >= 0}
     * @param to exclusive end index, or {@code null} for "through the end of the list"
     * @return the stage
     * @throws IllegalArgumentException when {@code to} is non-null and less than {@code from}
     * @param <T> element type
     */
    public static <T> @NotNull SubListCollect<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "From", placeholder = "0")
        int from,
        @Configurable(label = "To (exclusive, optional)", placeholder = "10", optional = true)
        @Nullable Integer to
    ) {
        int clampedFrom = Math.max(0, from);
        if (to != null && to < clampedFrom)
            throw new IllegalArgumentException(
                "SubListCollect 'to' (" + to + ") must be >= 'from' (" + clampedFrom + ")"
            );
        return new SubListCollect<>(elementType, DataType.list(elementType), clampedFrom, to);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        int end = (this.to == null) ? input.size() : Math.min(this.to, input.size());
        int start = Math.min(this.from, end);
        return Concurrent.newUnmodifiableList(input.subList(start, end));
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
        return "SubList[" + this.from + "," + (this.to == null ? "end" : this.to) + "] " + this.elementType.label();
    }

}
