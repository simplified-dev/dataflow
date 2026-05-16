package dev.simplified.dataflow.stage.source;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.SourceStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
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
@StageSpec(
    id = "SOURCE_EMBED",
    displayName = "Embed pipeline",
    description = "() -> O",
    category = StageSpec.Category.SOURCE
)
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
        @Configurable(label = "Saved pipeline id", placeholder = "wiki_dmg")
        @NotNull String embeddedPipelineId,
        @Configurable(label = "Output type", placeholder = "INT")
        @NotNull DataType<O> outputType
    ) {
        return new EmbedSource<>(embeddedPipelineId, outputType);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable O execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        ctx.enterPipeline(this.embeddedPipelineId);
        try {
            DataPipeline<?> inner = ctx.resolver().resolve(this.embeddedPipelineId).orElseThrow(() ->
                new IllegalStateException("Embedded pipeline not found or not accessible: '" + this.embeddedPipelineId + "'")
            );
            return inner.expectOutput(this.outputType).execute(ctx);
        } finally {
            ctx.exitPipeline(this.embeddedPipelineId);
        }
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Embed '" + this.embeddedPipelineId + "' (" + this.outputType.label() + ")";
    }

}
