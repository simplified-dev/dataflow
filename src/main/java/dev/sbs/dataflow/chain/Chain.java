package dev.sbs.dataflow.chain;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.Stage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
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
 */
public record Chain(@NotNull ConcurrentList<Stage<?, ?>> stages) {

    /**
     * Wraps {@code stages} as an immutable {@link Chain}.
     *
     * @param stages the body stages in execution order
     * @return a chain whose backing list is frozen
     */
    @SuppressWarnings("unchecked")
    public static @NotNull Chain of(@NotNull List<? extends Stage<?, ?>> stages) {
        return new Chain(Concurrent.newUnmodifiableList((List<Stage<?, ?>>) stages));
    }

    /**
     * Creates a fresh {@link ChainBuilder}.
     *
     * @return a new builder
     */
    public static @NotNull ChainBuilder builder() {
        return new ChainBuilder();
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
                    "Sub-pipeline stage #" + i + " (" + stage.kind() + ") expects input " + expected
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
     * Executes the chain against {@code input}, returning the final value with the type
     * inferred at the call site.
     * <p>
     * Mirrors {@link DataPipeline#execute(PipelineContext)}: the cast from runtime
     * {@link Object} to {@code T} is unchecked - the compiler trusts the inferred type.
     * A {@code null} from any stage short-circuits the remainder.
     *
     * @param ctx the pipeline context
     * @param input the value supplied by the enclosing stage
     * @return the final value, or {@code null} when a stage rejects its input
     * @param <T> inferred result type
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> @Nullable T execute(@NotNull PipelineContext ctx, @Nullable Object input) {
        Object current = input;
        for (Stage stage : this.stages) {
            if (current == null) break;
            current = stage.execute(ctx, current);
        }
        return (T) current;
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
