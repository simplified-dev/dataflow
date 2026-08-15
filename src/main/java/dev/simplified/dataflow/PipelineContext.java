package dev.simplified.dataflow;

import com.google.gson.Gson;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.BuilderIgnore;
import dev.simplified.annotations.ClassBuilder;
import dev.simplified.annotations.Collector;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.SetterNames;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.fetch.UrlFetcherConfig;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.EmbedSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * Per-execution state and dependencies threaded through a {@link DataPipeline}.
 * <p>
 * Discord-agnostic by design: holds a {@link UrlFetcher}, a {@link Logger}, a
 * {@link DataPipelineResolver}, and an opaque key/value bag that the host application can
 * use to attach whatever extra context it needs without invading the pipeline core. The
 * mutable {@code activeIds} set guards against {@link EmbedSource} cycles. An optional
 * tracing hook fires after every stage's {@code execute} for debugging / instrumentation.
 */
@Getter(style = NamingStyle.FLUENT)
@ClassBuilder(
    constructorAccess = AccessLevel.PRIVATE,
    setters = @SetterNames(set = "with{}", put = "with{}")
)
public final class PipelineContext {

    private static final @NotNull Logger DEFAULT_LOG = LoggerFactory.getLogger(PipelineContext.class);
    private static final @NotNull UrlFetcher DEFAULT_FETCHER = UrlFetcher.create(
        UrlFetcherConfig.builder(new Gson()).build()
    );

    private final @NotNull UrlFetcher fetcher = DEFAULT_FETCHER;
    private final @NotNull Logger log = DEFAULT_LOG;
    private final @NotNull DataPipelineResolver resolver = DataPipelineResolver.NOOP;

    @Collector(singular = true, singularMethodName = "BagEntry")
    private final @NotNull ConcurrentMap<String, Object> bag = Concurrent.newMap();

    @BuilderIgnore
    private final @NotNull ConcurrentSet<String> activeIds = Concurrent.newSet();

    /**
     * Per-stage callback fired after every stage's {@code execute}, in both top-level
     * {@link DataPipeline} and sub-chain {@code Chain} execution, receiving the stage that
     * just ran and its post-execute output - {@code null} installs no hook.
     * <p>
     * Useful for debugging: inspect intermediate values, time each stage, or assert in tests
     * that the expected stages ran in order. It fires even when a stage returns {@code null},
     * so traces capture rejection points.
     */
    private final @Nullable BiConsumer<Stage<?, ?>, Object> trace = null;

    /**
     * Convenience factory returning a fully defaulted context: default fetcher, default
     * logger, {@link DataPipelineResolver#NOOP NOOP} resolver, empty bag, no tracer.
     * Suitable for tests and ad-hoc usage where no host-supplied wiring is needed.
     *
     * @return a context with all dependencies set to their defaults
     */
    public static @NotNull PipelineContext defaults() {
        return builder().build();
    }

    /**
     * Fires the {@code trace} hook (if configured) after a stage's
     * {@code execute} has run. Both {@link DataPipeline#execute(PipelineContext)} and the
     * body walk inside {@code Chain.execute} call this; no-op when no tracer was installed.
     *
     * @param stage the stage that just ran
     * @param output the value the stage produced ({@code null} when rejected)
     */
    public void traceStage(@NotNull Stage<?, ?> stage, @Nullable Object output) {
        if (this.trace != null) this.trace.accept(stage, output);
    }

    /**
     * Records that the pipeline with the given id has begun executing inside this context.
     *
     * @param id the stable id of the embedded pipeline
     * @throws IllegalStateException if {@code id} is already in flight, indicating a cycle
     */
    public void enterPipeline(@NotNull String id) {
        if (!this.activeIds.add(id))
            throw new IllegalStateException(
                "Pipeline cycle detected entering '" + id + "'; already active: " + this.activeIds
            );
    }

    /**
     * Records that the pipeline with the given id has finished executing.
     *
     * @param id the stable id of the embedded pipeline
     */
    public void exitPipeline(@NotNull String id) {
        this.activeIds.remove(id);
    }

}
