package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns the first element of the input list, or {@code null}
 * when the list is empty.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "COLLECT_FIRST",
    displayName = "First",
    description = "List<T> -> T",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirstCollect<T> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a first-element collect stage.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull FirstCollect<T> of(
        @Configurable(label = "Element type", placeholder = "DOM_NODE")
        @NotNull DataType<T> elementType
    ) {
        return new FirstCollect<>(elementType, DataType.list(elementType));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty()) return null;
        return input.get(0);
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
        return "First " + this.elementType.label();
    }

}
