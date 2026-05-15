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
 * {@link TransformStage} that runs a sub-pipeline body once per element, mapping
 * {@code List<X>} to {@code List<Y>}. Elements whose body evaluation returns {@code null}
 * are dropped, mirroring the outer pipeline's rejection semantics.
 *
 * @param <X> input element type
 * @param <Y> output element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MapTransform<X, Y> implements TransformStage<List<X>, List<Y>> {

    private final @NotNull DataType<X> inputElementType;

    private final @NotNull DataType<Y> outputElementType;

    private final @NotNull DataType<List<X>> inputListType;

    private final @NotNull DataType<List<Y>> outputListType;

    private final @NotNull Chain body;

    /**
     * Constructs a map stage with the given element types and body chain.
     *
     * @param inputElementType element type of the input list
     * @param outputElementType element type of the output list
     * @param body the per-element sub-pipeline, consuming {@code X} and producing {@code Y}
     * @return the stage
     * @param <X> input element type
     * @param <Y> output element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <X, Y> @NotNull MapTransform<X, Y> of(
        @NotNull DataType<X> inputElementType,
        @NotNull DataType<Y> outputElementType,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(inputElementType, body, outputElementType);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid map body: " + report.issues());
        return new MapTransform<>(
            inputElementType,
            outputElementType,
            DataType.list(inputElementType),
            DataType.list(outputElementType),
            Chain.of(body)
        );
    }

    /**
     * Reconstructs a map stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull MapTransform<?, ?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> inputElementType = cfg.getDataType("elementInputType");
        DataType<?> outputElementType = cfg.getDataType("elementOutputType");
        Chain body = cfg.getSubPipeline("body");
        return of(inputElementType, outputElementType, body.stages());
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
        List<Y> result = new ArrayList<>(input.size());
        for (X element : input) {
            Y mapped = this.body.execute(ctx, element);
            if (mapped != null) result.add(mapped);
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
        return StageKind.TRANSFORM_MAP;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Y>> outputType() {
        return this.outputListType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Map " + this.inputElementType.label() + " -> " + this.outputElementType.label()
            + " (" + this.body.size() + " stage" + (this.body.size() == 1 ? "" : "s") + ")";
    }

}
