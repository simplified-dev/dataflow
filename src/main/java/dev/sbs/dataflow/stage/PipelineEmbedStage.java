package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Stage} that runs another {@link DataPipeline} as a single step.
 * Always source-like: its input type is {@link DataTypes#NONE}, and the inner pipeline is
 * a self-contained {@code DataPipeline} resolved by stable id from
 * {@link PipelineContext#resolver()} at execute-time.
 * <p>
 * Cycle detection is enforced by {@link PipelineContext#enterPipeline} and
 * {@link PipelineContext#exitPipeline}.
 *
 * @param <O> output type of the inner pipeline
 */
public non-sealed interface PipelineEmbedStage<O> extends Stage<Void, O> {

    /** {@inheritDoc} */
    @Override
    default @NotNull DataType<Void> inputType() {
        return DataTypes.NONE;
    }

    /**
     * Stable id of the embedded pipeline, used both for resolution and cycle detection.
     *
     * @return the embedded pipeline id
     */
    @NotNull String embeddedPipelineId();

}
