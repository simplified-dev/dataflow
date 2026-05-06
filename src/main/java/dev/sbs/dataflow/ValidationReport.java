package dev.sbs.dataflow;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Outcome of {@link DataPipeline#validate()}.
 *
 * @param issues every problem found, in walk order; an empty list means the pipeline is valid
 */
public record ValidationReport(@NotNull List<Issue> issues) {

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
     * True when no issues were reported.
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

}
