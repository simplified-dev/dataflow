package dev.simplified.dataflow.stage.transform.list;

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
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link TransformStage} that reads a list and an offset out of one value and rotates the list by
 * the offset.
 * <p>
 * Both bodies run against the same input. Output element {@code i} is input element
 * {@code Math.floorMod(i + offset, n)}, so the element at the offset comes first, a negative
 * offset rotates the other way, and an offset of {@code n} leaves the list as it is. An empty list
 * stays empty. Elements are carried as they are, {@code null} ones included.
 *
 * @param <I> input type, shared by both bodies
 * @param <T> element type
 */
@StageSpec(
    id = "TRANSFORM_ROTATE",
    displayName = "Rotate",
    description = "I -> List<T> (list: I -> List<T>, offset: I -> INT)",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RotateTransform<I, T> implements TransformStage<I, List<T>> {

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain<I, List<T>> list;

    private final @NotNull Chain<I, Integer> offset;

    /**
     * Constructs a rotate stage.
     *
     * @param inputType the input type both bodies consume
     * @param elementType element type of the list
     * @param list sub-pipeline that maps {@code I} to {@code List<T>}
     * @param offset sub-pipeline that maps {@code I} to {@code INT}, the index of the element
     *               that comes first
     * @return the stage
     * @param <I> input type
     * @param <T> element type
     * @throws IllegalArgumentException when either body fails type-chain validation
     */
    public static <I, T> @NotNull RotateTransform<I, T> of(
        @Configurable(label = "Input type", placeholder = "JSON_OBJECT")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "List body (yields List<T>)")
        @NotNull List<? extends Stage<?, ?>> list,
        @Configurable(label = "Offset body (yields INT)")
        @NotNull List<? extends Stage<?, ?>> offset
    ) {
        DataType<List<T>> listType = DataType.list(elementType);
        ValidationReport listReport = Chain.validate(inputType, list, listType);

        if (!listReport.isValid())
            throw new IllegalArgumentException("Invalid RotateTransform list body: " + listReport.issues());

        ValidationReport offsetReport = Chain.validate(inputType, offset, DataTypes.INT);

        if (!offsetReport.isValid())
            throw new IllegalArgumentException("Invalid RotateTransform offset body: " + offsetReport.issues());

        return new RotateTransform<>(inputType, elementType, listType, Chain.of(list), Chain.of(offset));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        List<T> values = this.list.execute(ctx, input);
        if (values == null) return null;
        Integer shift = this.offset.execute(ctx, input);
        if (shift == null) return null;
        int size = values.size();
        if (size == 0) return Concurrent.newUnmodifiableList();
        int first = Math.floorMod(shift, size);
        List<T> rotated = new ArrayList<>(size);
        rotated.addAll(values.subList(first, size));
        rotated.addAll(values.subList(0, first));
        return Concurrent.newUnmodifiableList(rotated);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Rotate " + this.inputType.label() + " -> " + this.listType.label()
            + " (list " + this.list.size() + ", offset " + this.offset.size() + " stages)";
    }

}
