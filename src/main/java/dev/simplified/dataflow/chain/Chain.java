package dev.simplified.dataflow.chain;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.SourceStage;
import dev.simplified.dataflow.stage.Stage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable, sourceless flat sequence of {@link Stage} instances - the body carried inside
 * another stage that fans an input value through a per-element or per-branch chain.
 * <p>
 * Differs from a {@link DataPipeline} in that a chain has no {@link SourceStage}: its first
 * stage's input is supplied by the enclosing stage at execute time. The same value-walk
 * semantics apply: a {@code null} from any stage short-circuits the rest of the chain.
 *
 * @param stages the ordered, immutable stages forming this chain
 * @param <I> input type expected by the chain's first stage
 * @param <O> output type produced by the chain's last stage
 */
public record Chain<I, O>(@NotNull ConcurrentList<Stage<?, ?>> stages) {

    /**
     * Wraps {@code stages} as an immutable {@link Chain} whose type parameters are inferred
     * at the call site from the enclosing stage's expectation. The caller asserts that the
     * stages form a well-typed chain consuming {@code I} and producing {@code O}; the
     * contract is checked dynamically via {@link Chain#validate}.
     *
     * @param stages the body stages in execution order
     * @return a chain whose backing list is frozen
     * @param <I> input type expected by the chain's first stage
     * @param <O> output type produced by the chain's last stage
     */
    @SuppressWarnings("unchecked")
    public static <I, O> @NotNull Chain<I, O> of(@NotNull List<? extends Stage<?, ?>> stages) {
        return new Chain<>(Concurrent.newUnmodifiableList((List<Stage<?, ?>>) stages));
    }

    /**
     * Serde entry that wraps {@code stages} as a fully-erased chain. Used by the wire-format
     * reader where neither the input nor output type is statically known.
     *
     * @param stages the body stages in execution order
     * @return a chain whose type parameters are wildcards
     */
    public static @NotNull Chain<?, ?> unchecked(@NotNull List<? extends Stage<?, ?>> stages) {
        return Chain.of(stages);
    }

    /**
     * Creates a fresh {@link ChainBuilder} seeded with the given input type. Each
     * {@code stage} call advances the running output phantom type, mirroring
     * {@link DataPipeline.Builder}.
     *
     * @param seedType the type the first stage must consume
     * @return a new builder running on {@code seedType}
     * @param <I> input type
     */
    public static <I> @NotNull ChainBuilder<I, I> builder(@NotNull DataType<I> seedType) {
        return new ChainBuilder<>(seedType);
    }

    /**
     * Walks {@code chain} and reports every type-chain mismatch.
     * <p>
     * The first stage in {@code chain} must consume {@code seedInputType} (supplied by the
     * enclosing stage) and the last must produce {@code expectedOutputType}. Empty chains
     * report a single pipeline-level "no stages" issue.
     *
     * @param seedInputType the type the first stage must consume
     * @param chain the body stages, in execution order
     * @param expectedOutputType the type the last stage must produce
     * @return the validation report
     */
    public static @NotNull ValidationReport validate(
        @NotNull DataType<?> seedInputType,
        @NotNull List<? extends Stage<?, ?>> chain,
        @NotNull DataType<?> expectedOutputType
    ) {
        if (chain.isEmpty())
            return ValidationReport.of(ValidationReport.Issue.pipelineLevel(
                "Sub-pipeline has no stages; expected at least one"
            ));

        List<ValidationReport.Issue> issues = new ArrayList<>();
        DataType<?> previousOutput = seedInputType;
        for (int i = 0; i < chain.size(); i++) {
            Stage<?, ?> stage = chain.get(i);
            DataType<?> expected = stage.inputType();
            if (!expected.equals(previousOutput))
                issues.add(new ValidationReport.Issue(i,
                    "Sub-pipeline stage #" + i + " (" + stage.kindId() + ") expects input " + expected
                        + " but previous stage produced " + previousOutput
                ));
            previousOutput = stage.outputType();
        }

        if (!expectedOutputType.equals(previousOutput))
            issues.add(ValidationReport.Issue.pipelineLevel(
                "Sub-pipeline produces " + previousOutput + " but caller expected " + expectedOutputType
            ));

        return new ValidationReport(List.copyOf(issues));
    }

    /**
     * Executes the chain against {@code input}.
     * <p>
     * Mirrors {@link DataPipeline#execute(PipelineContext)}: a {@code null} from any stage
     * short-circuits the remainder.
     *
     * @param ctx the pipeline context
     * @param input the value supplied by the enclosing stage
     * @return the final value, or {@code null} when a stage rejects its input
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public @Nullable O execute(@NotNull PipelineContext ctx, @Nullable I input) {
        Object current = input;
        for (Stage stage : this.stages) {
            if (current == null) break;
            current = stage.execute(ctx, current);
            ctx.traceStage(stage, current);
        }
        return (O) current;
    }

    /**
     * Number of stages in this chain.
     *
     * @return the stage count
     */
    public int size() {
        return this.stages.size();
    }

    /**
     * Whether this chain has zero stages.
     *
     * @return {@code true} when empty
     */
    public boolean isEmpty() {
        return this.stages.isEmpty();
    }

}
