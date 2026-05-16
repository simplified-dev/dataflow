package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageKind;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * {@link TransformStage} that sorts a list by the element type's natural ordering.
 * Supported element types: {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE},
 * {@code STRING}. {@code null} elements are dropped before sorting.
 *
 * @param <T> element type, must be {@link Comparable}
 */
@StageSpec(
    displayName = "Sort list",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SortTransform<T extends Comparable<T>> implements TransformStage<List<T>, List<T>> {

    private static final @NotNull Set<DataType<?>> SUPPORTED = Set.of(
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.STRING
    );

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final boolean ascending;

    /**
     * Constructs a sort stage.
     *
     * @param elementType element type of the list, must be one of the supported comparable types
     * @param ascending whether to sort in ascending order; {@code false} sorts descending
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code elementType} is not supported
     */
    public static <T extends Comparable<T>> @NotNull SortTransform<T> of(
        @Configurable(label = "Element type", placeholder = "INT")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Ascending", placeholder = "true")
        boolean ascending
    ) {
        if (!SUPPORTED.contains(elementType))
            throw new IllegalArgumentException(
                "SortTransform supports " + SUPPORTED + " but got " + elementType
            );
        return new SortTransform<>(elementType, DataType.list(elementType), ascending);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> result = new ArrayList<>(input.size());
        for (T element : input) {
            if (element != null) result.add(element);
        }
        Comparator<T> order = this.ascending ? Comparator.naturalOrder() : Comparator.reverseOrder();
        result.sort(order);
        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_SORT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Sort " + this.elementType.label() + " " + (this.ascending ? "asc" : "desc");
    }

}
