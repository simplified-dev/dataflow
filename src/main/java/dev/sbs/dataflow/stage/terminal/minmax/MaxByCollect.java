package dev.sbs.dataflow.stage.terminal.minmax;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns the element of the input list whose key (extracted via
 * the sub-pipeline body) is largest. Skips elements whose body yields {@code null}.
 * First-wins on ties.
 *
 * @param <T> element type
 * @param <K> key type, must be {@link Comparable}
 */
@StageSpec(
    id = "COLLECT_MAX_BY",
    displayName = "MaxBy (key)",
    description = "List<T> -> T (body: T -> K)",
    category = StageSpec.Category.TERMINAL_MINMAX
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MaxByCollect<T, K extends Comparable<K>> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<K> keyType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain body;

    /**
     * Constructs a max-by collect stage.
     *
     * @param elementType element type of the input list
     * @param keyType comparable key type produced by {@code body}
     * @param body sub-pipeline that maps {@code T} to {@code K}
     * @return the stage
     * @param <T> element type
     * @param <K> key type
     * @throws IllegalArgumentException when {@code keyType} is not supported or {@code body} fails type-chain validation
     */
    public static <T, K extends Comparable<K>> @NotNull MaxByCollect<T, K> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Key type", placeholder = "INT")
        @NotNull DataType<K> keyType,
        @Configurable(label = "Key extractor body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        if (!DataTypes.COMPARABLE_KEYS.contains(keyType))
            throw new IllegalArgumentException(
                "MaxByCollect supports key types " + DataTypes.COMPARABLE_KEYS + " but got " + keyType
            );
        ValidationReport report = Chain.validate(elementType, body, keyType);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid maxBy body: " + report.issues());
        return new MaxByCollect<>(
            elementType,
            keyType,
            DataType.list(elementType),
            Chain.of(body)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null || input.isEmpty()) return null;
        T best = null;
        K bestKey = null;
        for (T element : input) {
            K key = this.body.execute(ctx, element);
            if (key == null) continue;
            if (bestKey == null || key.compareTo(bestKey) > 0) {
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
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "MaxBy " + this.elementType.label() + " key=" + this.keyType.label();
    }

}
