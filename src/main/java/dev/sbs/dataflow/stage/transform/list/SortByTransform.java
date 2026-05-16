package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * {@link TransformStage} that sorts a list by a key extracted from each element via a
 * sub-pipeline body. Mirrors {@link Comparator#comparing}. Elements whose body
 * yields {@code null} are pushed to the end regardless of direction.
 *
 * @param <T> element type
 * @param <K> key type, must be {@link Comparable}
 */
@StageSpec(
    id = "TRANSFORM_SORT_BY",
    displayName = "Sort by key",
    description = "List<T> -> List<T> (body: T -> K)",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SortByTransform<T, K extends Comparable<K>> implements TransformStage<List<T>, List<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<K> keyType;

    private final @NotNull DataType<List<T>> listType;

    private final boolean ascending;

    private final @NotNull Chain body;

    /**
     * Constructs a sort-by stage.
     *
     * @param elementType element type of the input list
     * @param keyType the comparable key type produced by {@code body}; must be one of
     *                {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE}, {@code STRING}
     * @param ascending {@code true} for natural order; {@code false} for reverse
     * @param body sub-pipeline that maps {@code T} to {@code K}
     * @return the stage
     * @param <T> element type
     * @param <K> key type
     * @throws IllegalArgumentException when {@code keyType} is not supported or {@code body} fails type-chain validation
     */
    public static <T, K extends Comparable<K>> @NotNull SortByTransform<T, K> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Key type", placeholder = "INT")
        @NotNull DataType<K> keyType,
        @Configurable(label = "Ascending", placeholder = "true")
        boolean ascending,
        @Configurable(label = "Key extractor body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        if (!DataTypes.COMPARABLE_KEYS.contains(keyType))
            throw new IllegalArgumentException(
                "SortByTransform supports key types " + DataTypes.COMPARABLE_KEYS + " but got " + keyType
            );
        ValidationReport report = Chain.validate(elementType, body, keyType);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid sortBy body: " + report.issues());
        return new SortByTransform<>(
            elementType,
            keyType,
            DataType.list(elementType),
            ascending,
            Chain.of(body)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;

        record Keyed<T, K extends Comparable<K>>(T element, @Nullable K key) {}
        List<Keyed<T, K>> keyed = new ArrayList<>(input.size());
        for (T element : input) {
            K key = this.body.execute(ctx, element);
            keyed.add(new Keyed<>(element, key));
        }

        Comparator<K> order = this.ascending ? Comparator.naturalOrder() : Comparator.reverseOrder();
        keyed.sort(Comparator.comparing(Keyed::key, Comparator.nullsLast(order)));

        List<T> result = new ArrayList<>(keyed.size());
        for (Keyed<T, K> entry : keyed) result.add(entry.element());
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
        return "SortBy " + this.elementType.label() + " key=" + this.keyType.label()
            + " " + (this.ascending ? "asc" : "desc");
    }

}
