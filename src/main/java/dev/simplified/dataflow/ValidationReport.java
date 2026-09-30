package dev.simplified.dataflow;

import dev.simplified.dataflow.stage.source.EmbedSource;
import dev.simplified.dataflow.stage.transform.primitive.ExpectTransform;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Outcome of {@link DataPipeline#validate()}.
 * <p>
 * Issues decide validity; expectations do not. An expectation is the sentence an
 * {@link ExpectTransform} states about the values it will see, listed so a reader of the report
 * learns what the pipeline checks at run time before running it. A pipeline with expectations and
 * no issues is valid.
 * <p>
 * A saved pipeline an {@link EmbedSource} runs is resolved only when the embed executes, so the
 * report lists none of its expectations and does not mark the embed; they are listed by the report
 * of the resolved pipeline.
 *
 * @param issues every problem found, in walk order; an empty list means the pipeline is valid
 * @param expectations every expectation an {@link ExpectTransform} in the pipeline states, in walk
 *                     order, including those nested in bodies and operands and excluding those of a
 *                     saved pipeline an {@link EmbedSource} runs
 */
public record ValidationReport(@NotNull List<Issue> issues, @NotNull List<Expectation> expectations) {

    /**
     * Constructs a report of the given issues and no expectations.
     *
     * @param issues every problem found, in walk order
     */
    public ValidationReport(@NotNull List<Issue> issues) {
        this(issues, List.of());
    }

    /**
     * Sentinel report representing "no issues."
     *
     * @return an empty report
     */
    public static @NotNull ValidationReport ok() {
        return new ValidationReport(List.of());
    }

    /**
     * Constructs a report from a varargs sequence of issues.
     *
     * @param issues the issues
     * @return a report wrapping the supplied issues
     */
    public static @NotNull ValidationReport of(@NotNull Issue... issues) {
        return new ValidationReport(List.of(issues));
    }

    /**
     * True when no issues were reported. Expectations do not affect validity.
     *
     * @return whether the pipeline is valid
     */
    public boolean isValid() {
        return this.issues.isEmpty();
    }

    /**
     * Single problem found by the validator.
     *
     * @param stageIndex zero-based index of the offending stage, or {@code -1} for pipeline-level issues
     * @param message human-readable description of the problem
     */
    public record Issue(int stageIndex, @NotNull String message) {

        /**
         * Constructs a pipeline-level issue not tied to a specific stage.
         *
         * @param message the problem description
         * @return an issue with {@code stageIndex == -1}
         */
        public static @NotNull Issue pipelineLevel(@NotNull String message) {
            return new Issue(-1, message);
        }

    }

    /**
     * Expectation one {@link ExpectTransform} states, found where the stage sits in the pipeline.
     * <p>
     * The path names the stage's place in the wire form: {@code #} and the top-level stage index,
     * then for each level of nesting a dot, the slot's key, the branch name when the slot holds
     * named bodies (followed by {@code .chain} for a typed body), and the stage's index in that
     * stage array. {@code #3} is the fourth top-level stage, {@code #3.body[1]} the second stage of
     * its {@code body}, {@code #2.outputs.name.chain[0]} the first stage of the {@code name}
     * branch of a typed {@code outputs} slot, and {@code #1.right[2]} the third stage of a
     * {@code right} pipeline operand.
     *
     * @param stageIndex zero-based index of the top-level stage that is the expect stage or nests it
     * @param path where the expect stage sits, such as {@code #3.body[1]}
     * @param text the sentence the stage states
     * @param inputType type of the value the stage tests
     */
    public record Expectation(int stageIndex, @NotNull String path, @NotNull String text, @NotNull DataType<?> inputType) { }

}
