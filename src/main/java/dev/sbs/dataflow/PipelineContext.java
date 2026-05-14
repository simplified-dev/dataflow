package dev.sbs.dataflow;

import com.google.gson.Gson;
import dev.sbs.dataflow.stage.source.PipelineEmbed;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.fetch.UrlFetcherConfig;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-execution state and dependencies threaded through a {@link DataPipeline}.
 * <p>
 * Discord-agnostic by design: holds a {@link UrlFetcher}, a {@link Logger}, a
 * {@link DataPipelineResolver}, and an opaque key/value bag that the host application can
 * use to attach whatever extra context it needs without invading the pipeline core. The
 * mutable {@code activeIds} set guards against {@link PipelineEmbed} cycles.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PipelineContext {

    private static final @NotNull Logger DEFAULT_LOG = LoggerFactory.getLogger(PipelineContext.class);
    private static final @NotNull UrlFetcher DEFAULT_FETCHER = UrlFetcher.create(
        UrlFetcherConfig.builder(new Gson()).build()
    );

    private final @NotNull UrlFetcher fetcher;
    private final @NotNull Logger log;
    private final @NotNull DataPipelineResolver resolver;
    private final @NotNull ConcurrentMap<String, Object> bag;
    private final @NotNull ConcurrentSet<String> activeIds;

    /**
     * Convenience factory for tests and ad-hoc usage where no real resolver or bag is needed.
     *
     * @return a context with a default fetcher, default logger, NOOP resolver, and empty bag
     */
    public static @NotNull PipelineContext empty() {
        return builder().build();
    }

    /**
     * Creates a fresh {@link Builder} for assembling a context.
     *
     * @return a new builder
     */
    public static @NotNull Builder builder() {
        return new Builder();
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

    /**
     * Mutable builder for {@link PipelineContext}.
     */
    public static final class Builder {

        private @NotNull UrlFetcher fetcher = DEFAULT_FETCHER;
        private @NotNull Logger log = DEFAULT_LOG;
        private @NotNull DataPipelineResolver resolver = DataPipelineResolver.NOOP;
        private final @NotNull ConcurrentMap<String, Object> bag = Concurrent.newMap();

        private Builder() {}

        public @NotNull Builder withFetcher(@NotNull UrlFetcher fetcher) {
            this.fetcher = fetcher;
            return this;
        }

        public @NotNull Builder withLogger(@NotNull Logger log) {
            this.log = log;
            return this;
        }

        public @NotNull Builder withResolver(@NotNull DataPipelineResolver resolver) {
            this.resolver = resolver;
            return this;
        }

        public @NotNull Builder withBagEntry(@NotNull String key, @NotNull Object value) {
            this.bag.put(key, value);
            return this;
        }

        public @NotNull PipelineContext build() {
            return new PipelineContext(this.fetcher, this.log, this.resolver, this.bag, Concurrent.newSet());
        }

    }

}
