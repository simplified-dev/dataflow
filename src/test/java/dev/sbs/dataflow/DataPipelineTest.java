package dev.sbs.dataflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class DataPipelineTest {

    @Test
    @DisplayName("Empty pipeline reports a single 'no stages' diagnostic")
    void emptyPipelineHasNoSource() {
        ValidationReport report = DataPipeline.empty().validate();
        assertThat(report.isValid(), is(false));
        assertThat(report.issues(), hasSize(1));
        ValidationReport.Issue issue = report.issues().get(0);
        assertThat(issue.stageIndex(), is(equalTo(-1)));
        assertThat(issue.message(), containsString("no stages"));
    }

    @Test
    @DisplayName("Empty pipeline returns null on execute - the stage loop simply iterates zero stages")
    void emptyPipelineExecutesToNull() {
        Object result = DataPipeline.empty().execute(PipelineContext.defaults());
        assertThat(result, is(equalTo(null)));
    }

    @Test
    @DisplayName("Building an empty pipeline throws because validate() reports 'no stages'")
    void emptyBuilderRejectsBuild() {
        try {
            DataPipeline.builder().build();
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("invalid pipeline"));
            assertThat(expected.getMessage(), containsString("no stages"));
            return;
        }
        throw new AssertionError("Expected IllegalStateException");
    }

}
