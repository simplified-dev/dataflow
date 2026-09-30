package dev.simplified.dataflow;

import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link PipelineContext#derive(Object, Supplier)}: at most one derivation per key per
 * context, keyed by identity, with a {@code null} result held like any other, a fresh memo in
 * every context, a derivation that throws or reaches its own key, the tracer, and reads from a
 * second thread, which share one lock with operand evaluations.
 */
class PipelineContextDeriveTest {

    /**
     * Key the concurrent reads derive under.
     */
    private static final @NotNull Object KEY = new Object();

    @Test
    @DisplayName("derive answers the derivation's result")
    void answersDerivation() {
        assertThat(PipelineContext.defaults().derive(new Object(), () -> "x"), is(equalTo("x")));
    }

    @Test
    @DisplayName("A value is derived once per context however many times it is read")
    void derivedOncePerContext() {
        Object key = new Object();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = PipelineContext.defaults();

        ctx.derive(key, runs::incrementAndGet);
        ctx.derive(key, runs::incrementAndGet);
        ctx.derive(key, runs::incrementAndGet);

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A second read answers the held value")
    void secondReadAnswersHeldValue() {
        Object key = new Object();
        PipelineContext ctx = PipelineContext.defaults();
        List<String> first = ctx.derive(key, ArrayList::new);
        List<String> second = ctx.derive(key, ArrayList::new);
        assertThat(second, is(sameInstance(first)));
    }

    @Test
    @DisplayName("A null result is held and not derived again")
    void nullResultIsHeld() {
        Object key = new Object();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = PipelineContext.defaults();

        ctx.derive(key, () -> {
            runs.incrementAndGet();
            return null;
        });
        ctx.derive(key, () -> {
            runs.incrementAndGet();
            return "x";
        });

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A null result reads back as null")
    void nullResultReadsBackNull() {
        Object key = new Object();
        PipelineContext ctx = PipelineContext.defaults();
        ctx.derive(key, () -> null);
        assertThat(ctx.derive(key, () -> "x"), is(nullValue()));
    }

    @Test
    @DisplayName("Keys are compared by identity: two equal keys derive twice")
    @SuppressWarnings("StringOperationCanBeSimplified")
    void keyedByIdentity() {
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = PipelineContext.defaults();

        ctx.derive(new String("key"), runs::incrementAndGet);
        ctx.derive(new String("key"), runs::incrementAndGet);

        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("Each context derives afresh")
    void eachContextDerivesAfresh() {
        Object key = new Object();
        AtomicInteger runs = new AtomicInteger();

        PipelineContext.defaults().derive(key, runs::incrementAndGet);
        PipelineContext.defaults().derive(key, runs::incrementAndGet);

        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("A context made by mutate() starts with nothing derived")
    void mutatedContextHoldsNothing() {
        Object key = new Object();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = PipelineContext.defaults();

        ctx.derive(key, runs::incrementAndGet);
        ctx.mutate().build().derive(key, runs::incrementAndGet);

        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("A value derived under a pipeline is apart from that pipeline's operand result")
    void apartFromOperandMemo() {
        DataPipeline<String> operand = DataPipeline.builder().source(LiteralSource.text("operand")).build();
        PipelineContext ctx = PipelineContext.defaults();

        ctx.evaluateOperand(operand);

        assertThat(ctx.derive(operand, () -> "derived"), is(equalTo("derived")));
    }

    @Test
    @DisplayName("A derivation that throws holds nothing, so the next read derives again")
    void failedDerivationIsNotHeld() {
        Object key = new Object();
        PipelineContext ctx = PipelineContext.defaults();
        assertThrows(IllegalStateException.class, () -> ctx.derive(key, () -> {
            throw new IllegalStateException("failed");
        }));

        assertThat(ctx.derive(key, () -> "x"), is(equalTo("x")));
    }

    @Test
    @DisplayName("A derivation that reaches its own key again is a cycle")
    void derivationCycleDetected() {
        Object key = new Object();
        PipelineContext ctx = PipelineContext.defaults();
        AtomicReference<Runnable> reenter = new AtomicReference<>();
        reenter.set(() -> ctx.derive(key, () -> {
            reenter.get().run();
            return "x";
        }));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> reenter.get().run());
        assertThat(thrown.getMessage(), containsString("Derived value cycle detected"));
    }

    @Test
    @DisplayName("A derivation that reads another key derives both")
    void nestedDerivationDerives() {
        Object outer = new Object();
        Object inner = new Object();
        PipelineContext ctx = PipelineContext.defaults();

        String value = ctx.derive(outer, () -> "a" + ctx.derive(inner, () -> "b"));

        assertThat(value, is(equalTo("ab")));
    }

    @Test
    @DisplayName("A derivation reaches no tracer")
    void derivationIsNotTraced() {
        List<Object> traced = new ArrayList<>();
        PipelineContext ctx = PipelineContext.builder().withTrace((stage, output) -> traced.add(stage)).build();

        ctx.derive(new Object(), () -> "x");

        assertThat(traced, is(empty()));
    }

    @Test
    @DisplayName("A read from a second thread during a derivation waits for it rather than deriving again")
    void concurrentReadDoesNotDeriveAgain() throws InterruptedException {
        ConcurrentReads reads = concurrentReads(ctx -> ctx.derive(KEY, () -> "second"));
        assertThat(reads.runs(), is(1));
    }

    @Test
    @DisplayName("A read from a second thread during a derivation answers the value that derivation held")
    void concurrentReadAnswersHeldValue() throws InterruptedException {
        ConcurrentReads reads = concurrentReads(ctx -> ctx.derive(KEY, () -> "second"));
        assertThat(reads.second(), is(sameInstance(reads.first())));
    }

    @Test
    @DisplayName("An operand read from a second thread during a derivation waits for it, under the one lock")
    void concurrentOperandReadWaits() throws InterruptedException {
        DataPipeline<String> operand = DataPipeline.builder().source(LiteralSource.text("operand")).build();
        ConcurrentReads reads = concurrentReads(ctx -> ctx.evaluateOperand(operand));
        assertThat(reads.second(), is(equalTo("operand")));
    }

    /**
     * What two threads read from one context while one of them derives the value under
     * {@link #KEY}.
     *
     * @param runs the number of times the value under {@link #KEY} was derived
     * @param first the value the deriving thread read
     * @param second the value the thread that read during the derivation received
     */
    private record ConcurrentReads(int runs, @Nullable Object first, @Nullable Object second) {}

    /**
     * Derives the value under {@link #KEY} on one thread and, while the derivation runs, reads the
     * context through {@code read} on a second thread, holding the derivation until the second
     * thread is waiting on it.
     *
     * @param read the read the second thread makes
     * @return the derivation's run count and the value each thread read
     * @throws InterruptedException when interrupted while waiting for the second thread
     */
    private static @NotNull ConcurrentReads concurrentReads(@NotNull Function<PipelineContext, Object> read) throws InterruptedException {
        PipelineContext ctx = PipelineContext.defaults();
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<Object> second = new AtomicReference<>();
        Thread reader = new Thread(() -> second.set(read.apply(ctx)));

        Object first = ctx.derive(KEY, () -> {
            runs.incrementAndGet();
            reader.start();
            awaitBlocked(reader);
            return new Object();
        });

        reader.join(TimeUnit.SECONDS.toMillis(10));
        return new ConcurrentReads(runs.get(), first, second.get());
    }

    /**
     * Waits until {@code thread} is blocked entering a monitor.
     *
     * @param thread the thread to watch
     * @throws AssertionError when it is not blocked within ten seconds
     */
    private static void awaitBlocked(@NotNull Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (thread.getState() != Thread.State.BLOCKED) {
            if (System.nanoTime() > deadline)
                throw new AssertionError("The second reader never waited on the derivation under way");

            Thread.onSpinWait();
        }
    }

}
