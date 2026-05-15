package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LastCollect<T> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a last-element collect stage.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull LastCollect<T> of(@NotNull DataType<T> elementType) {
        return new LastCollect<>(elementType, DataType.list(elementType));
    }

    /**
     * Reconstructs the collect from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LastCollect<?> fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getDataType("elementType"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty()) return null;
        return input.get(input.size() - 1);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_LAST;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Last " + this.elementType.label();
    }

}
