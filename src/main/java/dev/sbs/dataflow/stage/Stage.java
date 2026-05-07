package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One step in a {@link DataPipeline}, mapping an input value of type
 * {@code I} to an output value of type {@code O}.
 * <p>
 * Each stage advertises its {@link #inputType()} and {@link #outputType()} as runtime
 * {@link DataType} descriptors. The pipeline's type-chain validator uses these to detect
 * mismatches before execution. The serde discriminator is {@link #kind()}, which never
 * changes once a stored definition is written.
 *
 * @param <I> the runtime input type
 * @param <O> the runtime output type
 */
public sealed interface Stage<I, O> permits
    SourceStage, FilterStage, TransformStage, BranchStage, CollectStage, PipelineEmbedStage {

    /**
     * Runtime descriptor of the input value this stage expects.
     *
     * @return the input type
     */
    @NotNull DataType<I> inputType();

    /**
     * Runtime descriptor of the value this stage produces.
     *
     * @return the output type
     */
    @NotNull DataType<O> outputType();

    /**
     * Stable discriminator used in the serialised definition.
     *
     * @return the kind
     */
    @NotNull StageKind kind();

    /**
     * Configuration of this stage exposed as a typed name-to-value map. Mirrors the schema
     * declared on {@link #kind()}.
     * <p>
     * Both serde (writes) and the UI builder (reads for pre-fill, writes when constructing
     * a replacement via {@link StageKind#factory()}) iterate this single source of truth,
     * eliminating per-kind switches. Default returns {@link StageConfig#empty()} for
     * config-free stages.
     *
     * @return the configuration
     */
    default @NotNull StageConfig config() {
        return StageConfig.empty();
    }

    /**
     * One-line label used by the UI to describe this stage's configuration. Keep it short
     * enough to fit inside a Discord {@code Section}.
     *
     * @return the summary
     */
    @NotNull String summary();

    /**
     * Executes this stage against {@code input}.
     *
     * @param ctx the pipeline context
     * @param input the input value, or {@code null} for source stages
     * @return the output value, possibly {@code null} when the stage rejects the input
     */
    @Nullable O execute(@NotNull PipelineContext ctx, @Nullable I input);

}
