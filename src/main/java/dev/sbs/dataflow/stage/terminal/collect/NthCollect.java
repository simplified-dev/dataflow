package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns the element at a fixed index of the input list, or
 * {@code null} when the list is empty or the index is past the end.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "COLLECT_NTH",
    displayName = "Nth",
    description = "List<T> -> T",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NthCollect<T> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final int index;

    /**
     * Constructs an nth-element collect stage.
     *
     * @param elementType element type of the list
     * @param index zero-based position of the element to return; clamped to {@code >= 0}
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull NthCollect<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Index", placeholder = "0")
        int index
    ) {
        return new NthCollect<>(elementType, DataType.list(elementType), Math.max(0, index));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty() || this.index >= input.size()) return null;
        return input.get(this.index);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Nth[" + this.index + "] " + this.elementType.label();
    }

}
