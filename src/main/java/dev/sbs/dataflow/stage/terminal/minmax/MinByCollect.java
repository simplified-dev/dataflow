package dev.sbs.dataflow.stage.terminal.minmax;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.StageChainValidator;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Stage;
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
import java.util.Set;

/**
 * {@link CollectStage} that returns the element of the input list whose key (extracted via
 * the sub-pipeline body) is smallest. Skips elements whose body yields {@code null}.
 * First-wins on ties.
 *
 * @param <T> element type
 * @param <K> key type, must be {@link Comparable}
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinByCollect<T, K extends Comparable<K>> implements CollectStage<List<T>, T> {

    private static final @NotNull Set<DataType<?>> SUPPORTED_KEYS = Set.of(
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.STRING
    );

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<K> keyType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull ConcurrentList<Stage<?, ?>> body;

    /**
     * Constructs a min-by collect stage.
     *
     * @param elementType element type of the input list
     * @param keyType comparable key type produced by {@code body}
     * @param body sub-pipeline that maps {@code T} to {@code K}
     * @return the stage
     * @param <T> element type
     * @param <K> key type
     * @throws IllegalArgumentException when {@code keyType} is not supported or {@code body} fails type-chain validation
     */
    public static <T, K extends Comparable<K>> @NotNull MinByCollect<T, K> of(
        @NotNull DataType<T> elementType,
        @NotNull DataType<K> keyType,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        if (!SUPPORTED_KEYS.contains(keyType))
            throw new IllegalArgumentException(
                "MinByCollect supports key types " + SUPPORTED_KEYS + " but got " + keyType
            );
        ValidationReport report = StageChainValidator.validate(elementType, body, keyType);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid minBy body: " + report.issues());
        return new MinByCollect<>(
            elementType,
            keyType,
            DataType.list(elementType),
            Concurrent.newUnmodifiableList((List<Stage<?, ?>>) body)
        );
    }

    /**
     * Reconstructs a min-by stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull MinByCollect<?, ?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> elementType = cfg.getDataType("elementType");
        DataType<?> keyType = cfg.getDataType("keyType");
        ConcurrentList<Stage<?, ?>> body = cfg.getSubPipeline("body");
        return of((DataType) elementType, (DataType) keyType, body);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .dataType("keyType", this.keyType)
            .subPipeline("body", this.body)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty()) return null;
        T best = null;
        K bestKey = null;
        for (T element : input) {
            Object current = element;
            for (Stage stage : this.body) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            if (current == null) continue;
            K key = (K) current;
            if (bestKey == null || key.compareTo(bestKey) < 0) {
                best = element;
                bestKey = key;
            }
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
        return StageKind.COLLECT_MIN_BY;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "MinBy " + this.elementType.label() + " key=" + this.keyType.label();
    }

}
