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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link CollectStage} that converts a list into a {@code Set} preserving first-occurrence
 * order, dropping duplicates by {@link Object#equals(Object)}.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "COLLECT_SET",
    displayName = "Set",
    description = "List<T> -> Set<T>",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SetCollect<T> implements CollectStage<List<T>, Set<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull DataType<Set<T>> setType;

    /**
     * Constructs a collect-to-set stage.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull SetCollect<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType
    ) {
        return new SetCollect<>(elementType, DataType.list(elementType), DataType.set(elementType));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Set<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        return Set.copyOf(new LinkedHashSet<>(input));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Set<T>> outputType() {
        return this.setType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Set " + this.elementType.label();
    }

}
