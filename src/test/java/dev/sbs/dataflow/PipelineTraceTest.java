package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.source.LiteralSource;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import dev.sbs.dataflow.stage.transform.string.UpperCaseTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class PipelineTraceTest {

    @Test
    @DisplayName("Tracer fires for every top-level and sub-chain stage in execution order")
    void traceCapturesAllStages() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "a,b"))
            .stage(SplitTransform.of(","))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.STRING, List.of(UpperCaseTransform.of())))
            .build();

        List<String> trace = new ArrayList<>();
        BiConsumer<Stage<?, ?>, Object> recorder = (stage, output) ->
            trace.add(stage.kind().name() + "=" + output);

        PipelineContext ctx = PipelineContext.builder().withTrace(recorder).build();
        List<String> result = pipeline.execute(ctx);

        assertThat(result, is(equalTo(List.of("A", "B"))));
        assertThat(trace, contains(
            "SOURCE_LITERAL=a,b",
            "TRANSFORM_SPLIT=[a, b]",
            "TRANSFORM_UPPERCASE=A",
            "TRANSFORM_UPPERCASE=B",
            "TRANSFORM_MAP=[A, B]"
        ));
    }

    @Test
    @DisplayName("Tracer fires with null output when a stage rejects its input")
    void traceCapturesNullOnRejection() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "not-a-number"))
            .stage(dev.sbs.dataflow.stage.transform.string.RegexExtractTransform.of("\\d+"))
            .stage(dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform.of())
            .build();

        List<Object> trace = new ArrayList<>();
        PipelineContext ctx = PipelineContext.builder()
            .withTrace((stage, output) -> trace.add(output))
            .build();

        Object result = pipeline.execute(ctx);

        assertThat(result, is(org.hamcrest.Matchers.nullValue()));
        // SOURCE_LITERAL produced "not-a-number"; REGEX_EXTRACT yielded null which short-circuited.
        assertThat(trace, contains("not-a-number", null));
    }

    @Test
    @DisplayName("Context without a tracer (defaults()) executes pipelines unaffected")
    void defaultsHasNoTracer() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "hi"))
            .stage(UpperCaseTransform.of())
            .build();

        String result = pipeline.execute();
        assertThat(result, is(equalTo("HI")));
    }

}
