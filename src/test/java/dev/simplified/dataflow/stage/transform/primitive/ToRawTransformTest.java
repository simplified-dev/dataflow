package dev.simplified.dataflow.stage.transform.primitive;

import com.google.gson.JsonElement;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.json.PathTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ToRawTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("A null input stays null")
    void nullInNullOut() {
        assertThat(ToRawTransform.of(DataTypes.RAW_HTML).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("The input passes through as the same String")
    void passesInputThrough() {
        String input = "<p>a</p>";
        assertThat(ToRawTransform.of(DataTypes.RAW_HTML).execute(this.ctx, input), is(sameInstance(input)));
    }

    @Test
    @DisplayName("A malformed body passes through; nothing is parsed")
    void parsesNothing() {
        assertThat(ToRawTransform.of(DataTypes.RAW_JSON).execute(this.ctx, "{not json"), is(equalTo("{not json")));
    }

    @Test
    @DisplayName("The input type is STRING")
    void inputIsString() {
        assertThat(ToRawTransform.of(DataTypes.RAW_XML).inputType(), is(sameInstance(DataTypes.STRING)));
    }

    @TestFactory
    @DisplayName("The output type is the configured raw type")
    Stream<DynamicTest> outputIsConfiguredRawType() {
        return Stream.of(DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON).map(type -> DynamicTest.dynamicTest(
            type.label(),
            () -> assertThat(ToRawTransform.of(type).outputType(), is(sameInstance(type)))
        ));
    }

    @Test
    @DisplayName("of refuses STRING, which is not a raw type")
    void refusesString() {
        assertThrows(IllegalArgumentException.class, () -> ToRawTransform.of(DataTypes.STRING));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    @DisplayName("of refuses a type that is not String-backed")
    void refusesNonStringType() {
        DataType<String> intAsRaw = (DataType) DataTypes.INT;
        assertThrows(IllegalArgumentException.class, () -> ToRawTransform.of(intAsRaw));
    }

    @Test
    @DisplayName("A STRING retyped as RAW_JSON reaches PARSE_JSON")
    void feedsParseStage() {
        DataPipeline<JsonElement> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("{\"a\":{\"b\":7}}"))
            .stage(ToRawTransform.of(DataTypes.RAW_JSON))
            .stage(ParseJsonTransform.of())
            .stage(PathTransform.of("a.b"))
            .build();
        assertThat(pipeline.execute(this.ctx).getAsInt(), is(equalTo(7)));
    }

    @Test
    @DisplayName("The wire form names the raw type")
    void wireForm() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("<p>a</p>"))
            .stage(ToRawTransform.of(DataTypes.RAW_HTML))
            .build();
        assertThat(PipelineGson.toJson(pipeline), containsString("{\"kind\":\"TRANSFORM_TO_RAW\",\"outputType\":\"RAW_HTML\"}"));
    }

    @Test
    @DisplayName("A pipeline with the stage round-trips to the same JSON")
    void wireRoundTripIsStable() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("<p>a</p>"))
            .stage(ToRawTransform.of(DataTypes.RAW_HTML))
            .build();
        String first = PipelineGson.toJson(pipeline);
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with the stage round-trips to the same output type")
    void wireRoundTripKeepsOutputType() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("<p>a</p>"))
            .stage(ToRawTransform.of(DataTypes.RAW_HTML))
            .build();
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(pipeline));
        assertThat(rebuilt.stages().getLast().outputType(), is(sameInstance(DataTypes.RAW_HTML)));
    }

    @Test
    @DisplayName("A pipeline with the stage round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("<p>a</p>"))
            .stage(ToRawTransform.of(DataTypes.RAW_HTML))
            .build();
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(pipeline));
        assertThat(rebuilt.execute(this.ctx), is(equalTo(pipeline.execute(this.ctx))));
    }

    @Test
    @DisplayName("A non-raw output type on the wire fails the load")
    void wireRejectsNonRawType() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TRANSFORM_TO_RAW\",\"outputType\":\"STRING\"}]";
        assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
    }

}
