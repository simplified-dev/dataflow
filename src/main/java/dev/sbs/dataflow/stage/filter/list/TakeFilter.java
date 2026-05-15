package dev.sbs.dataflow.stage.filter.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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
 * {@link FilterStage} keeping the first {@code n} elements of the list.
 *
 * @param <T> element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TakeFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final int count;

    /**
     * Constructs a take-first-n filter.
     *
     * @param elementType element type of the list
     * @param count how many leading elements to keep
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull TakeFilter<T> of(@NotNull DataType<T> elementType, int count) {
        return new TakeFilter<>(elementType, DataType.list(elementType), Math.max(0, count));
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull TakeFilter<?> fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getDataType("elementType"), cfg.getInt("count"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .integer("count", this.count)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        return Concurrent.newUnmodifiableList(input.subList(0, Math.min(this.count, input.size())));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_TAKE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Take " + this.count;
    }

}
