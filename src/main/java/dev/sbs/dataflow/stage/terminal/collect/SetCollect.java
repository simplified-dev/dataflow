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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link CollectStage} that converts a list into a {@code Set} preserving first-occurrence
 * order, dropping duplicates by {@link Object#equals(Object)}.
 *
 * @param <T> element type
 */
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
    public static <T> @NotNull SetCollect<T> of(@NotNull DataType<T> elementType) {
        return new SetCollect<>(elementType, DataType.list(elementType), DataType.set(elementType));
    }

    /**
     * Reconstructs the collect from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull SetCollect<?> fromConfig(@NotNull StageConfig cfg) {
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
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_SET;
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
