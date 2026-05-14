package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.Stage;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Type-chain validator for a sourceless sub-pipeline carried inside another stage's
 * configuration, such as the body of a map / flatMap / takeWhile.
 * <p>
 * Differs from {@link DataPipeline#validate()} in two ways: the chain has no
 * {@link dev.sbs.dataflow.stage.SourceStage} (its first stage's input comes from the
 * outer stage, not from a source), and the final stage's output type is checked against
 * a caller-supplied expected type.
 */
@UtilityClass
public final class StageChainValidator {

    /**
     * Walks {@code chain} and reports every type-chain mismatch.
     *
     * @param seedInputType the type the first stage in {@code chain} must consume
     * @param chain the stages forming the sub-pipeline body, in execution order
     * @param expectedOutputType the type the last stage in {@code chain} must produce
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

}
