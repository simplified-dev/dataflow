package dev.sbs.dataflow.stage.terminal.minmax;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
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
import java.util.Set;

/**
 * {@link CollectStage} that returns the smallest element in the input list using the
 * element type's natural ordering. Supported element types: {@code INT}, {@code LONG},
 * {@code FLOAT}, {@code DOUBLE}, {@code STRING}.
 *
 * @param <T> element type, must be {@link Comparable}
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinCollect<T extends Comparable<T>> implements CollectStage<List<T>, T> {

    private static final @NotNull Set<DataType<?>> SUPPORTED = Set.of(
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.STRING
    );

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a min stage for the given element type. Element type must be one of the
     * supported comparable types.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code elementType} is not supported
     */
    public static <T extends Comparable<T>> @NotNull MinCollect<T> of(@NotNull DataType<T> elementType) {
        if (!SUPPORTED.contains(elementType))
            throw new IllegalArgumentException(
                "MinCollect supports " + SUPPORTED + " but got " + elementType
            );
        return new MinCollect<>(elementType, DataType.list(elementType));
    }

    /**
     * Reconstructs a min stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull MinCollect<?> fromConfig(@NotNull StageConfig cfg) {
        return of((DataType) cfg.getDataType("elementType"));
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
        T best = null;
        for (T element : input) {
            if (element == null) continue;
            if (best == null || element.compareTo(best) < 0) best = element;
        }
        return best;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_MIN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Min " + this.elementType.label();
    }

}
