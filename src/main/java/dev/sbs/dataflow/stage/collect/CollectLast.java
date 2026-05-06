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

import java.util.List;

/**
 * {@link CollectStage} that returns the last element of the input list, or {@code null}
 * when the list is empty.
 *
 * @param <T> element type
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class CollectLast<T> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;
    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a last-element collect stage.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull CollectLast<T> of(@NotNull DataType<T> elementType) {
        return new CollectLast<>(elementType, DataType.list(elementType));
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
    public @NotNull StageId kind() {
        return StageId.COLLECT_LAST;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Last " + this.elementType.label();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty()) return null;
        return input.get(input.size() - 1);
    }

}
