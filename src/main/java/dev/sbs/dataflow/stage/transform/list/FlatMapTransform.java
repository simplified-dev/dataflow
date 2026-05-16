package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.Stage;
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
import java.util.List;
import java.util.stream.Stream;

/**
 * {@link TransformStage} that runs a sub-pipeline body once per element where each body
 * evaluation yields a {@code List<Y>}, concatenating the per-element lists into a single
 * {@code List<Y>}. Mirrors {@link Stream#flatMap}.
 *
 * @param <X> input element type
 * @param <Y> output element type
 */
@StageSpec(
    displayName = "FlatMap sub-pipeline",
    description = "List<X> -> List<Y> (body: X -> List<Y>)",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FlatMapTransform<X, Y> implements TransformStage<List<X>, List<Y>> {

    private final @NotNull DataType<X> inputElementType;

    private final @NotNull DataType<Y> outputElementType;

    private final @NotNull DataType<List<X>> inputListType;

    private final @NotNull DataType<List<Y>> outputListType;

    private final @NotNull Chain body;

    /**
     * Constructs a flatMap stage with the given element types and body chain. The body's
     * final stage must produce {@code List<Y>}.
     *
     * @param inputElementType element type of the input list
     * @param outputElementType element type of the output list
     * @param body the per-element sub-pipeline, consuming {@code X} and producing {@code List<Y>}
     * @return the stage
     * @param <X> input element type
     * @param <Y> output element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <X, Y> @NotNull FlatMapTransform<X, Y> of(
        @Configurable(label = "Input element type", placeholder = "STRING", name = "elementInputType")
        @NotNull DataType<X> inputElementType,
        @Configurable(label = "Output element type", placeholder = "STRING", name = "elementOutputType")
        @NotNull DataType<Y> outputElementType,
        @Configurable(label = "Per-element body (yields List<Y>)")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(
            inputElementType,
            body,
            DataType.list(outputElementType)
        );
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid flatMap body: " + report.issues());
        return new FlatMapTransform<>(
            inputElementType,
            outputElementType,
            DataType.list(inputElementType),
            DataType.list(outputElementType),
            Chain.of(body)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Y> execute(@NotNull PipelineContext ctx, @Nullable List<X> input) {
        if (input == null) return null;
        List<Y> result = new ArrayList<>();
        for (X element : input) {
            List<Y> sub = this.body.execute(ctx, element);
            if (sub != null) result.addAll(sub);
        }
        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<X>> inputType() {
        return this.inputListType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_FLAT_MAP;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Y>> outputType() {
        return this.outputListType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "FlatMap " + this.inputElementType.label() + " -> List<" + this.outputElementType.label() + ">"
            + " (" + this.body.size() + " stage" + (this.body.size() == 1 ? "" : "s") + ")";
    }

}
