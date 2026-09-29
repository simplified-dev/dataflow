package dev.simplified.dataflow.stage.filter.list;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.FilterStage;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link FilterStage} that keeps one element per key, the key extracted from each element by a
 * sub-pipeline body.
 * <p>
 * The first element carrying a key is kept, or the last when {@code keepLast} is set, and the
 * kept elements stay in their input order. An element whose body yields {@code null} is dropped.
 * Keys compare by {@link Object#equals(Object)}, as {@link DistinctFilter} compares whole
 * elements.
 *
 * @param <T> element type
 * @param <K> key type
 */
@StageSpec(
    id = "FILTER_DISTINCT_BY",
    displayName = "Distinct by key",
    description = "List<T> -> List<T> (body: T -> K)",
    category = StageSpec.Category.FILTER_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DistinctByFilter<T, K> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<K> keyType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain<T, K> body;

    /**
     * Configured keep-last flag exactly as given, carried on the wire under {@code keepLast}, or
     * {@code null} when the slot is absent.
     */
    private final @Nullable Boolean rawKeepLast;

    /**
     * Whether the last element per key is kept rather than the first.
     */
    private final boolean keepLast;

    /**
     * Constructs a distinct-by filter.
     *
     * @param elementType element type of the list
     * @param keyType the key type produced by {@code body}; one of {@code INT}, {@code LONG},
     *                {@code FLOAT}, {@code DOUBLE} or {@code STRING}
     * @param body sub-pipeline that maps {@code T} to {@code K}
     * @param rawKeepLast {@code true} to keep the last element per key, or {@code null} to keep
     *                    the first; carried on the wire as {@code keepLast}
     * @return the stage
     * @param <T> element type
     * @param <K> key type
     * @throws IllegalArgumentException when {@code keyType} is not supported or {@code body} fails type-chain validation
     */
    public static <T, K> @NotNull DistinctByFilter<T, K> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Key type", placeholder = "STRING")
        @NotNull DataType<K> keyType,
        @Configurable(label = "Key extractor body")
        @NotNull List<? extends Stage<?, ?>> body,
        @Configurable(name = "keepLast", label = "Keep last (optional)", placeholder = "false", optional = true)
        @Nullable Boolean rawKeepLast
    ) {
        if (!DataTypes.COMPARABLE_KEYS.contains(keyType)) {
            throw new IllegalArgumentException(String.format(
                "DistinctByFilter supports key types %s but got '%s'", DataTypes.COMPARABLE_KEYS, keyType
            ));
        }

        ValidationReport report = Chain.validate(elementType, body, keyType);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid DistinctByFilter body: " + report.issues());

        return new DistinctByFilter<>(
            elementType,
            keyType,
            DataType.list(elementType),
            Chain.of(body),
            rawKeepLast,
            Boolean.TRUE.equals(rawKeepLast)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> elements = new ArrayList<>(input);
        List<K> keys = new ArrayList<>(elements.size());

        for (T element : elements)
            keys.add(this.body.execute(ctx, element));

        int size = elements.size();
        boolean[] kept = new boolean[size];
        Set<K> seen = new HashSet<>();

        for (int step = 0; step < size; step++) {
            int index = this.keepLast ? size - 1 - step : step;
            K key = keys.get(index);
            kept[index] = key != null && seen.add(key);
        }

        List<T> result = new ArrayList<>(seen.size());

        for (int index = 0; index < size; index++)
            if (kept[index]) result.add(elements.get(index));

        return Concurrent.newUnmodifiableList(result);
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
    public @NotNull String summary() {
        return "DistinctBy " + this.elementType.label() + " key=" + this.keyType.label()
            + (this.keepLast ? " keep last" : " keep first");
    }

}
