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

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Per-execution state and dependencies threaded through a {@link DataPipeline}.
 * <p>
 * Discord-agnostic by design: holds a {@link UrlFetcher}, a {@link FetchGuard} over what it
 * fetches, a {@link Logger}, a {@link DataPipelineResolver}, and an opaque key/value bag that the
 * host application can use to attach whatever extra context it needs without invading the
 * pipeline core. The mutable {@code activeIds} set guards against {@link EmbedSource} cycles, a
 * per-context operand memo lets {@link #evaluateOperand(DataPipeline)} run each pipeline operand
 * at most once, and a per-context derivation memo lets {@link #derive(Object, Supplier)} build
 * each value a stage derives from them at most once. An optional tracing hook fires after every
 * stage's {@code execute} for debugging / instrumentation.
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
    private static final @NotNull Object HELD_NULL = new Object();
    private static final @NotNull Object IN_FLIGHT = new Object();

    private final @NotNull UrlFetcher fetcher = DEFAULT_FETCHER;
    private final @NotNull Logger log = DEFAULT_LOG;
    private final @NotNull DataPipelineResolver resolver = DataPipelineResolver.NOOP;

    /**
     * Check every fetching stage runs over each body it hands on, before handing it on - a throw
     * fails the run. {@link FetchGuard#NOOP} accepts every body.
     */
    private final @NotNull FetchGuard fetchGuard = FetchGuard.NOOP;

    @Collector(singular = true, singularMethodName = "BagEntry")
    private final @NotNull ConcurrentMap<String, Object> bag = Concurrent.newMap();

    @BuilderIgnore
    private final @NotNull ConcurrentSet<String> activeIds = Concurrent.newSet();

    /**
     * Results of the pipeline operands evaluated in this context, keyed by operand identity and
     * guarded by the map's own monitor. An operand that produced {@code null} holds
     * {@link #HELD_NULL}, and one whose evaluation is under way holds {@link #IN_FLIGHT}.
     */
    @BuilderIgnore
    @Getter(AccessLevel.NONE)
    private final @NotNull Map<DataPipeline<?>, Object> operands = new IdentityHashMap<>();

    /**
     * Values derived in this context through {@link #derive(Object, Supplier)}, keyed by the
     * identity of the key each is held under and guarded by the monitor of {@link #operands}, so
     * a derivation that reads an operand and an operand whose stages derive a value take one lock
     * and cannot wait on each other. A derivation that produced {@code null} holds
     * {@link #HELD_NULL}, and one under way holds {@link #IN_FLIGHT}.
     */
    @BuilderIgnore
    @Getter(AccessLevel.NONE)
    private final @NotNull Map<Object, Object> derived = new IdentityHashMap<>();

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
     * Convenience factory returning a fully defaulted context: default fetcher,
     * {@link FetchGuard#NOOP NOOP} fetch guard, default logger,
     * {@link DataPipelineResolver#NOOP NOOP} resolver, empty bag, no tracer.
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

    /**
     * Evaluates a pipeline operand against this context, at most once per context.
     * <p>
     * The first call for an operand runs it through {@link DataPipeline#execute(PipelineContext)}
     * against this context - the same fetcher, resolver, bag, tracer and {@link EmbedSource}
     * cycle guard - and holds its result, {@code null} included. Every later call for the same
     * operand answers the held result without running it again. Operands are keyed by identity,
     * so two separately built pipelines with the same stages are two operands. The results are
     * held for the life of the context, so a context serves one run: a context reused for a
     * later run answers it with the operand values the first run read.
     * <p>
     * An operand whose own evaluation reads another operand evaluates that one the same way. An
     * operand whose evaluation reaches itself again is a cycle and throws. An evaluation that
     * throws holds nothing, so the next call for the operand runs it again. Evaluations on one
     * context are serialised: a call from a second thread waits for the one in progress.
     *
     * @param operand the operand pipeline
     * @return the operand's output, or {@code null} when one of its stages rejected
     * @param <T> the operand's output type
     * @throws IllegalStateException when {@code operand} is already being evaluated in this
     *         context, indicating a cycle
     */
    public <T> @Nullable T evaluateOperand(@NotNull DataPipeline<T> operand) {
        return this.memoize(
            this.operands,
            operand,
            () -> operand.execute(this),
            () -> "Pipeline operand cycle detected; operand " + describe(operand) + " is already being evaluated"
        );
    }

    /**
     * Answers the value held under {@code key} in this context, deriving it on the first call for
     * that key.
     * <p>
     * A stage holds here what it derives from an operand once per run - an index of the operand's
     * rows, say - so the value lives as long as the context and no longer. The first call for a
     * key runs {@code derivation} and holds its result, {@code null} included; every later call
     * for the same key answers the held result without running it again. Keys are compared by
     * identity, so a stage keys its value by an object of its own and two stages never share one
     * by accident. A derivation is no stage and reaches no tracer; an operand it reads through
     * {@link #evaluateOperand(DataPipeline)} traces its own stages as any read does.
     * <p>
     * Derivations follow the rules of operand evaluations, under the same lock: a derivation that
     * reaches its own key again is a cycle and throws, one that throws holds nothing, so the next
     * call for the key runs it again, and a call from a second thread waits for the derivation or
     * evaluation in progress. Every new context, one {@code mutate().build()} makes included,
     * starts with nothing derived.
     *
     * @param key the key the value is held under, compared by identity
     * @param derivation the function deriving the value on the first call for {@code key}
     * @return the value held under {@code key}, or {@code null} when the derivation produced
     *         {@code null}
     * @param <T> the value's type
     * @throws IllegalStateException when {@code key} is already being derived in this context,
     *         indicating a cycle
     */
    public <T> @Nullable T derive(@NotNull Object key, @NotNull Supplier<? extends T> derivation) {
        return this.memoize(
            this.derived,
            key,
            derivation,
            () -> "Derived value cycle detected; the value under key '" + key + "' is already being derived"
        );
    }

    /**
     * Answers the result {@code memo} holds for {@code key}, computing and holding it on the
     * first call, under the monitor of {@link #operands}.
     *
     * @param memo the memo holding results by key identity
     * @param key the key the result is held under
     * @param computation the function computing the result on the first call for {@code key}
     * @param cycle the message of the exception thrown when {@code key} is already being computed
     * @return the result held under {@code key}
     * @param <K> the key's type
     * @param <T> the result's type
     * @throws IllegalStateException when {@code key} is already being computed in this context
     */
    @SuppressWarnings("unchecked")
    private <K, T> @Nullable T memoize(
        @NotNull Map<K, Object> memo,
        @NotNull K key,
        @NotNull Supplier<? extends T> computation,
        @NotNull Supplier<String> cycle
    ) {
        synchronized (this.operands) {
            Object held = memo.get(key);

            if (held == IN_FLIGHT)
                throw new IllegalStateException(cycle.get());

            if (held != null)
                return held == HELD_NULL ? null : (T) held;

            memo.put(key, IN_FLIGHT);
            boolean computed = false;

            try {
                T result = computation.get();
                memo.put(key, result == null ? HELD_NULL : result);
                computed = true;
                return result;
            } finally {
                if (!computed) memo.remove(key);
            }
        }
    }

    private static @NotNull String describe(@NotNull DataPipeline<?> operand) {
        if (operand.stages().isEmpty()) return "'<empty>'";
        return "'" + operand.stages().getFirst().summary() + "' (" + operand.stages().size() + " stages)";
    }

}
