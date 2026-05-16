package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.serde.PipelineGson;
import dev.sbs.dataflow.stage.source.LiteralSource;
import dev.sbs.dataflow.stage.transform.list.ListLengthTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Acceptance smoke tests for the new {@link StageRegistry}.
 */
class StageRegistryTest {

    @Test
    @DisplayName("Registry discovers at least the 134 concrete stage classes from the brief")
    void registrySizeMatches() {
        assertThat(StageRegistry.allOrdered().size(), is(greaterThanOrEqualTo(134)));
    }

    @Test
    @DisplayName("byId('TRANSFORM_LIST_LENGTH') returns ListLengthTransform")
    void byIdResolvesKnown() {
        assertThat(StageRegistry.byId("TRANSFORM_LIST_LENGTH"), is(sameInstance(ListLengthTransform.class)));
    }

    @Test
    @DisplayName("byId on an unknown id throws IllegalArgumentException naming the missing id")
    void byIdRejectsUnknown() {
        try {
            StageRegistry.byId("NOT_A_REAL_ID");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("NOT_A_REAL_ID"));
            return;
        }
        throw new AssertionError("Expected IllegalArgumentException for unknown id");
    }

    @Test
    @DisplayName("Wire format byte-identity: two consecutive serialisations yield the same JSON")
    void wireFormatStable() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "  hi  "))
            .stage(TrimTransform.of())
            .build();
        String first = PipelineGson.toJson(pipeline);
        String second = PipelineGson.toJson(pipeline);
        assertThat(second, is(equalTo(first)));
        // Existing id strings remain stable on the wire.
        assertThat(first, containsString("\"kind\":\"SOURCE_LITERAL\""));
        assertThat(first, containsString("\"kind\":\"TRANSFORM_TRIM\""));
    }

}
