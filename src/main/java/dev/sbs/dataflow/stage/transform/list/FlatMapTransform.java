package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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

/**
 * {@link TransformStage} that runs a sub-pipeline body once per element where each body
 * evaluation yields a {@code List<Y>}, concatenating the per-element lists into a single
 * {@code List<Y>}. Mirrors {@link java.util.stream.Stream#flatMap}.
 *
 * @param <X> input element type
 * @param <Y> output element type
 */
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
        @NotNull DataType<X> inputElementType,
        @NotNull DataType<Y> outputElementType,
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

    /**
     * Reconstructs a flatMap stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull FlatMapTransform<?, ?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> inputElementType = cfg.getDataType("elementInputType");
        DataType<?> outputElementType = cfg.getDataType("elementOutputType");
        Chain body = cfg.getSubPipeline("body");
        return of((DataType) inputElementType, (DataType) outputElementType, body.stages());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementInputType", this.inputElementType)
            .dataType("elementOutputType", this.outputElementType)
            .subPipeline("body", this.body)
            .build();
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
