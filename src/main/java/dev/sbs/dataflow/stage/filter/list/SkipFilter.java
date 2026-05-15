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
 * {@link FilterStage} dropping the first {@code n} elements of the list.
 *
 * @param <T> element type
 */
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
    public static <T> @NotNull SkipFilter<T> of(@NotNull DataType<T> elementType, int count) {
        return new SkipFilter<>(elementType, DataType.list(elementType), Math.max(0, count));
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull SkipFilter<?> fromConfig(@NotNull StageConfig cfg) {
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
    public @NotNull StageKind kind() {
        return StageKind.FILTER_SKIP;
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
