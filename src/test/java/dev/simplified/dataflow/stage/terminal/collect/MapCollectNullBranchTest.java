package dev.simplified.dataflow.stage.terminal.collect;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers a {@link MapCollect} branch that yields {@code null}: its name is left out of the
 * result. Before, the null reached {@code Map.copyOf} and the whole run threw
 * {@link NullPointerException}.
 */
class MapCollectNullBranchTest {

    private static @NotNull MapCollect<String> collect() {
        return MapCollect.over(DataTypes.STRING)
            .output("digits", c -> c.stage(RegexExtractTransform.of("\\d+")))
            .output("length", c -> c.stage(LengthTransform.of()))
            .build();
    }

    @Test
    @DisplayName("A branch yielding null is omitted from the result")
    void nullBranchOmitted() {
        assertThat(collect().execute(PipelineContext.defaults(), "abc"), not(hasKey("digits")));
    }

    @Test
    @DisplayName("The other branches still land in the result")
    void otherBranchesKept() {
        assertThat(collect().execute(PipelineContext.defaults(), "abc"), hasEntry("length", 3));
    }

    @Test
    @DisplayName("A pipeline ending in the collect completes when a branch misses")
    void pipelineCompletes() {
        DataPipeline<Map<String, Object>> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("abc"))
            .stage(collect())
            .build();
        assertThat(pipeline.execute(), is(equalTo(Map.of("length", 3))));
    }

}
