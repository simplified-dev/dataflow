package dev.sbs.dataflow;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Bridge that lets {@link dev.sbs.dataflow.stage.PipelineEmbedStage} look up another saved
 * {@link DataPipeline} at execute-time without the {@code dataflow} module knowing about
 * persistence, authorisation, or any specific storage layer.
 * <p>
 * The host application supplies an implementation - typically wrapping a database repository
 * with its own visibility/permission rules - and registers it on {@link PipelineContext}.
 */
public interface DataPipelineResolver {

    /**
     * No-op resolver that fails every lookup. Suitable for transient pipelines that never
     * embed another pipeline.
     */
    @NotNull DataPipelineResolver NOOP = new DataPipelineResolver() {

        @Override
        public @NotNull Optional<DataPipeline> resolve(@NotNull String id) {
            return Optional.empty();
        }

        @Override
        public @Nullable String idOf(@NotNull DataPipeline pipeline) {
            return null;
        }

    };

    /**
     * Resolves a saved pipeline by stable id, applying whatever permission rules the
     * implementation enforces. An empty result means "not found, or caller cannot use it" -
     * the caller does not get to distinguish between the two.
     *
     * @param id the stable pipeline id
     * @return the pipeline if accessible to the caller, otherwise empty
     */
    @NotNull Optional<DataPipeline> resolve(@NotNull String id);

    /**
     * Returns the stable id of {@code pipeline}, or {@code null} for an unsaved or
     * transient pipeline. Used by the cycle guard in {@link PipelineContext}.
     *
     * @param pipeline the pipeline to identify
     * @return the stable id, or {@code null} if unsaved
     */
    @Nullable String idOf(@NotNull DataPipeline pipeline);

}
