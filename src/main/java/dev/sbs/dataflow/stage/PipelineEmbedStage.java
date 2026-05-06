package dev.sbs.dataflow.stage;

import org.jetbrains.annotations.NotNull;

/**
 * {@link Stage} that runs another {@link dev.sbs.dataflow.DataPipeline} as a single step.
 * The inner pipeline is resolved from {@link dev.sbs.dataflow.PipelineContext#resolver()}
 * by stable id; the host application controls visibility, permissions, and storage.
 * <p>
 * Cycle detection is enforced by {@link dev.sbs.dataflow.PipelineContext#enterPipeline} and
 * {@link dev.sbs.dataflow.PipelineContext#exitPipeline}.
 *
 * @param <I> input type
 * @param <O> output type
 */
public non-sealed interface PipelineEmbedStage<I, O> extends Stage<I, O> {

    /**
     * Stable id of the embedded pipeline, used both for resolution and cycle detection.
     *
     * @return the embedded pipeline id
     */
    @NotNull String embeddedPipelineId();

}
