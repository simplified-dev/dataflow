package dev.sbs.dataflow.stage.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.StageId;
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
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
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
    public static <T> @NotNull SetCollect<T> of(@NotNull DataType<T> elementType) {
        return new SetCollect<>(elementType, DataType.list(elementType), DataType.set(elementType));
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
    public @NotNull StageId kind() {
        return StageId.COLLECT_SET;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Set " + this.elementType.label();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Set<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        return Set.copyOf(new LinkedHashSet<>(input));
    }

}
