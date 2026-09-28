package dev.simplified.dataflow.stage.transform.list;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link TransformStage} that appends the elements of a second document, read through a
 * pipeline operand, after the input's.
 * <p>
 * The output holds the input's elements and then the operand's, each in order, with nothing
 * de-duplicated; one stage appends one document, so {@code N} documents take {@code N - 1}
 * stages. The operand is evaluated at most once per context. A {@code null} input or a
 * {@code null} operand output rejects with {@code null}.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "TRANSFORM_CONCAT",
    displayName = "Concat",
    description = "List<T> -> List<T>",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConcatTransform<T> implements TransformStage<List<T>, List<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataPipeline<List<T>> other;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a stage appending the elements {@code other} produces.
     *
     * @param elementType element type of the input, the operand and the output
     * @param other the operand pipeline, whose output must be a {@code List} of {@code elementType}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code other} is invalid or does not produce a
     *         {@code List} of {@code elementType}
     */
    @SuppressWarnings("unchecked")
    public static <T> @NotNull ConcatTransform<T> of(
        @Configurable(label = "Element type", placeholder = "JSON_OBJECT")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Other pipeline")
        @NotNull DataPipeline<?> other
    ) {
        DataType<List<T>> listType = DataType.list(elementType);
        ValidationReport report = other.validate(listType);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid ConcatTransform operand: " + report.issues());

        return new ConcatTransform<>(elementType, (DataPipeline<List<T>>) other, listType);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> tail = ctx.evaluateOperand(this.other);
        if (tail == null) return null;
        List<T> result = new ArrayList<>(input.size() + tail.size());
        result.addAll(input);
        result.addAll(tail);
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
        return "Concat " + this.elementType.label() + " (" + this.other.stages().size() + " operand stages)";
    }

}
