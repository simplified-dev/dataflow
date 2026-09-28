package dev.simplified.dataflow;

import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.fixture.AppendOperandTransform;
import dev.simplified.dataflow.stage.source.EmbedSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link PipelineContext#evaluateOperand(DataPipeline)}: at most one evaluation per
 * context, keyed by operand identity, with a {@code null} result held like any other, nested
 * operands, and both the operand and the embed cycle guards.
 */
class PipelineContextOperandTest {

    private static final class MapResolver implements DataPipelineResolver {

        private final @NotNull Map<String, DataPipeline<?>> pipelines = new HashMap<>();

        @Override
        public @NotNull Optional<DataPipeline<?>> resolve(@NotNull String id) {
            return Optional.ofNullable(this.pipelines.get(id));
        }

        @Override
        public @Nullable String idOf(@NotNull DataPipeline<?> pipeline) {
            return null;
        }

    }

    /**
     * Builds a context whose tracer counts the executions of {@code watched}.
     */
    private static @NotNull PipelineContext counting(@NotNull Stage<?, ?> watched, @NotNull AtomicInteger runs) {
        return PipelineContext.builder()
            .withTrace((stage, output) -> {
                if (stage == watched) runs.incrementAndGet();
            })
            .build();
    }

    @Test
    @DisplayName("evaluateOperand answers the operand's output")
    void answersOperandOutput() {
        DataPipeline<String> operand = DataPipeline.builder().source(LiteralSource.text("x")).build();
        assertThat(PipelineContext.defaults().evaluateOperand(operand), is(equalTo("x")));
    }

    @Test
    @DisplayName("An operand runs once per context however many times it is read")
    void runsOncePerContext() {
        LiteralSource<String> source = LiteralSource.text("x");
        DataPipeline<String> operand = DataPipeline.builder().source(source).build();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(source, runs);

        ctx.evaluateOperand(operand);
        ctx.evaluateOperand(operand);
        ctx.evaluateOperand(operand);

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A second read answers the held value")
    void secondReadAnswersHeldValue() {
        DataPipeline<String> operand = DataPipeline.builder().source(LiteralSource.text("x")).build();
        PipelineContext ctx = PipelineContext.defaults();
        String first = ctx.evaluateOperand(operand);
        assertThat(ctx.evaluateOperand(operand), is(sameInstance(first)));
    }

    @Test
    @DisplayName("A null result is held and not re-evaluated")
    void nullResultIsHeld() {
        LiteralSource<String> source = LiteralSource.text("abc");
        DataPipeline<String> operand = DataPipeline.builder()
            .source(source)
            .stage(RegexExtractTransform.of("\\d+"))
            .build();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(source, runs);

        ctx.evaluateOperand(operand);
        ctx.evaluateOperand(operand);

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A null result reads back as null")
    void nullResultReadsBackNull() {
        DataPipeline<String> operand = DataPipeline.builder()
            .source(LiteralSource.text("abc"))
            .stage(RegexExtractTransform.of("\\d+"))
            .build();
        PipelineContext ctx = PipelineContext.defaults();
        ctx.evaluateOperand(operand);
        assertThat(ctx.evaluateOperand(operand), is(nullValue()));
    }

    @Test
    @DisplayName("Operands are keyed by identity: two pipelines over one source run twice")
    void keyedByIdentity() {
        LiteralSource<String> source = LiteralSource.text("x");
        DataPipeline<String> first = DataPipeline.builder().source(source).build();
        DataPipeline<String> second = DataPipeline.builder().source(source).build();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(source, runs);

        ctx.evaluateOperand(first);
        ctx.evaluateOperand(second);

        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("Each context evaluates an operand afresh, including one made by mutate()")
    void mutatedContextHoldsNothing() {
        LiteralSource<String> source = LiteralSource.text("x");
        DataPipeline<String> operand = DataPipeline.builder().source(source).build();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(source, runs);

        ctx.evaluateOperand(operand);
        ctx.mutate().build().evaluateOperand(operand);

        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("A stage reading its operand for several inputs runs it once per context")
    void stageReadsOperandOncePerRun() {
        LiteralSource<String> source = LiteralSource.text("!");
        DataPipeline<String> operand = DataPipeline.builder().source(source).build();
        AppendOperandTransform append = AppendOperandTransform.of(operand);
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(source, runs);

        append.execute(ctx, "a");
        append.execute(ctx, "b");
        append.execute(ctx, "c");

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("An operand whose evaluation reads another operand evaluates both")
    void nestedOperandEvaluates() {
        DataPipeline<String> inner = DataPipeline.builder().source(LiteralSource.text("b")).build();
        DataPipeline<String> outer = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(AppendOperandTransform.of(inner))
            .build();

        assertThat(PipelineContext.defaults().evaluateOperand(outer), is(equalTo("ab")));
    }

    @Test
    @DisplayName("A nested operand is held too: reading it after its parent does not run it again")
    void nestedOperandIsHeld() {
        LiteralSource<String> innerSource = LiteralSource.text("b");
        DataPipeline<String> inner = DataPipeline.builder().source(innerSource).build();
        DataPipeline<String> outer = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(AppendOperandTransform.of(inner))
            .build();
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = counting(innerSource, runs);

        ctx.evaluateOperand(outer);
        ctx.evaluateOperand(inner);

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A null operand output rejects the consuming stage and stops the pipeline")
    void nullOperandRejectsConsumer() {
        DataPipeline<String> operand = DataPipeline.builder()
            .source(LiteralSource.text("abc"))
            .stage(RegexExtractTransform.of("\\d+"))
            .build();
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("y"))
            .stage(AppendOperandTransform.of(operand))
            .build();

        assertThat(pipeline.execute(PipelineContext.defaults()), is(nullValue()));
    }

    @Test
    @DisplayName("An operand reached again during its own evaluation is a cycle")
    void operandCycleDetected() {
        MapResolver resolver = new MapResolver();
        DataPipeline<String> operand = DataPipeline.builder().source(EmbedSource.of("B", DataTypes.STRING)).build();
        resolver.pipelines.put("B", DataPipeline.builder()
            .source(LiteralSource.text("x"))
            .stage(AppendOperandTransform.of(operand))
            .build());
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("y"))
            .stage(AppendOperandTransform.of(operand))
            .build();
        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> pipeline.execute(ctx));
        assertThat(thrown.getMessage(), containsString("Pipeline operand cycle detected"));
    }

    @Test
    @DisplayName("The embed cycle guard still fires when the embed is reached through an operand")
    void embedCycleGuardThroughOperand() {
        MapResolver resolver = new MapResolver();
        DataPipeline<String> operand = DataPipeline.builder().source(EmbedSource.of("A", DataTypes.STRING)).build();
        resolver.pipelines.put("A", DataPipeline.builder()
            .source(LiteralSource.text("x"))
            .stage(AppendOperandTransform.of(DataPipeline.builder().source(EmbedSource.of("A", DataTypes.STRING)).build()))
            .build());
        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> ctx.evaluateOperand(operand));
        assertThat(thrown.getMessage(), containsString("Pipeline cycle detected entering 'A'"));
    }

    @Test
    @DisplayName("An operand that throws holds nothing, so the next read evaluates it again")
    void failedEvaluationIsNotHeld() {
        DataPipeline<String> operand = DataPipeline.builder().source(EmbedSource.of("missing", DataTypes.STRING)).build();
        PipelineContext ctx = PipelineContext.defaults();
        assertThrows(IllegalStateException.class, () -> ctx.evaluateOperand(operand));

        IllegalStateException second = assertThrows(IllegalStateException.class, () -> ctx.evaluateOperand(operand));
        assertThat(second.getMessage(), containsString("Embedded pipeline not found"));
    }

    @Test
    @DisplayName("An operand's stages report to the context's tracer")
    void operandStagesAreTraced() {
        LiteralSource<String> source = LiteralSource.text("x");
        DataPipeline<String> operand = DataPipeline.builder().source(source).build();
        AtomicInteger runs = new AtomicInteger();
        counting(source, runs).evaluateOperand(operand);
        assertThat(runs.get(), is(1));
    }

}
