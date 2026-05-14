package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SourceStage} that resolves another {@link DataPipeline} by stable id and runs it
 * as a single step.
 * <p>
 * The author declares the expected output type at construction so the chain validator can
 * verify downstream stages without having to load the inner pipeline. Mismatches at
 * runtime - e.g. the saved pipeline now produces a different type - surface as a
 * {@link ClassCastException} when the outer pipeline tries to use the value.
 * <p>
 * The {@link #embeddedPipelineId()} accessor stays on this concrete class for tooling
 * (UI, dependency-graph analysis) that wants to inspect the embed without execution: use
 * {@code if (stage instanceof EmbedSource<?> embed) ...}.
 *
 * @param <O> output type of the inner pipeline
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class EmbedSource<O> implements SourceStage<O> {

    private final @NotNull String embeddedPipelineId;

    private final @NotNull DataType<O> outputType;

    /**
     * Constructs an embed stage referencing the saved pipeline with id {@code embeddedPipelineId}
     * whose declared output type is {@code outputType}.
     *
     * @param embeddedPipelineId the saved pipeline id
     * @param outputType the declared output type of the inner pipeline
     * @return the stage
     * @param <O> output type
     */
    public static <O> @NotNull EmbedSource<O> of(
        @NotNull String embeddedPipelineId,
        @NotNull DataType<O> outputType
    ) {
        return new EmbedSource<>(embeddedPipelineId, outputType);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("embeddedPipelineId", this.embeddedPipelineId)
            .dataType("outputType", this.outputType)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public @Nullable O execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        ctx.enterPipeline(this.embeddedPipelineId);
        try {
            DataPipeline inner = ctx.resolver().resolve(this.embeddedPipelineId).orElseThrow(() ->
                new IllegalStateException("Embedded pipeline not found or not accessible: '" + this.embeddedPipelineId + "'")
            );
            return (O) inner.execute(ctx);
        } finally {
            ctx.exitPipeline(this.embeddedPipelineId);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.SOURCE_EMBED;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Embed '" + this.embeddedPipelineId + "' (" + this.outputType.label() + ")";
    }

}
