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
 * Identity {@link CollectStage} that returns the input list unchanged. Useful as an
 * explicit terminal marker in the pipeline UI when you want a {@code List<T>} result.
 *
 * @param <T> element type
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class CollectList<T> implements CollectStage<List<T>, List<T>> {

    private final @NotNull DataType<T> elementType;
    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs an identity collect stage.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull CollectList<T> of(@NotNull DataType<T> elementType) {
        return new CollectList<>(elementType, DataType.list(elementType));
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
        return StageId.COLLECT_LIST;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "List " + this.elementType.label();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        return input;
    }

}
