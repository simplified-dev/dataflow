package dev.simplified.dataflow;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.EmbedSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.FieldTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.json.StringifyTransform;
import dev.simplified.dataflow.stage.transform.list.ConcatTransform;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers the JSON widening every type check applies through {@link DataType#isAssignableTo}: a
 * {@code JSON_OBJECT} or {@code JSON_ARRAY} satisfies an expected {@code JSON_ELEMENT}, and a
 * {@code List} or {@code Set} satisfies one of a type its element widens to - between stages of a
 * pipeline, at the seed and the end of a body, for an operand, and when a pipeline is narrowed.
 */
class TypeWideningTest {

    private static final @NotNull DataType<List<JsonObject>> ROWS = DataType.list(DataTypes.JSON_OBJECT);

    private static final @NotNull DataType<List<JsonElement>> ELEMENTS = DataType.list(DataTypes.JSON_ELEMENT);

    private static final @NotNull String WIRE_ROWS = "{'kind':'SOURCE_LITERAL','outputType':'RAW_JSON','value':'[{\\'a\\':1},{\\'a\\':2}]'},"
        + "{'kind':'PARSE_JSON'},"
        + "{'kind':'TRANSFORM_JSON_DESERIALIZE','inputType':'JSON_ELEMENT','outputType':'List<JSON_OBJECT>'}";

    private static @NotNull String q(@NotNull String json) {
        return json.replace('\'', '"');
    }

    private static @NotNull DataPipeline.Builder<JsonObject> object() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson("{\"a\":1}"))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataTypes.JSON_OBJECT));
    }

    private static @NotNull DataPipeline<List<JsonObject>> rows() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson("[{\"a\":1},{\"a\":2}]"))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(ROWS))
            .build();
    }

    @Nested
    @DisplayName("Between the stages of a pipeline")
    class BetweenStages {

        @Test
        @DisplayName("a stage consuming JSON_ELEMENT accepts a JSON_OBJECT")
        void jsonObjectFeedsJsonElement() {
            assertThat(object().stage(StringifyTransform.of()).validate().isValid(), is(true));
        }

        @Test
        @DisplayName("a stage consuming JSON_ELEMENT accepts a JSON_ARRAY")
        void jsonArrayFeedsJsonElement() {
            ValidationReport report = DataPipeline.builder()
                .source(LiteralSource.rawJson("[1]"))
                .stage(ParseJsonTransform.of())
                .stage(DeserializeTransform.of(DataTypes.JSON_ARRAY))
                .stage(StringifyTransform.of())
                .validate();
            assertThat(report.isValid(), is(true));
        }

        @Test
        @DisplayName("a widened pipeline runs on the JsonObject it was handed")
        void widenedPipelineRuns() {
            assertThat(object().stage(StringifyTransform.of()).build().execute(), is(equalTo("{\"a\":1}")));
        }

        @Test
        @DisplayName("a stage consuming List<JSON_ELEMENT> loads and runs after a List<JSON_OBJECT>")
        void rowsFeedElementList() {
            DataPipeline<?> pipeline = PipelineGson.fromJson(q("[" + WIRE_ROWS + ",{'kind':'TRANSFORM_MAP','elementInputType':'JSON_ELEMENT',"
                + "'elementOutputType':'STRING','body':[{'kind':'TRANSFORM_JSON_STRINGIFY'}]}]"));
            assertThat(pipeline.execute(), is(equalTo(List.of("{\"a\":1}", "{\"a\":2}"))));
        }

        @Test
        @DisplayName("a stage consuming JSON_OBJECT still refuses a JSON_ELEMENT")
        void jsonElementDoesNotFeedJsonObject() {
            DataPipeline<?> pipeline = DataPipeline.unchecked(
                List.of(LiteralSource.rawJson("{\"a\":1}"), ParseJsonTransform.of(), FieldTransform.of("a")),
                DataTypes.JSON_ELEMENT
            );
            assertThat(pipeline.validate().isValid(), is(false));
        }

    }

    @Nested
    @DisplayName("Chain.validate")
    class ChainValidate {

        @Test
        @DisplayName("accepts a JSON_OBJECT seed for a body whose first stage consumes JSON_ELEMENT")
        void seedWidens() {
            assertThat(Chain.validate(DataTypes.JSON_OBJECT, List.of(StringifyTransform.of()), DataTypes.STRING).isValid(), is(true));
        }

        @Test
        @DisplayName("accepts a body producing JSON_OBJECT where JSON_ELEMENT is expected")
        void expectedOutputWidens() {
            ValidationReport report = Chain.validate(
                DataTypes.JSON_ELEMENT, List.of(DeserializeTransform.of(DataTypes.JSON_OBJECT)), DataTypes.JSON_ELEMENT
            );
            assertThat(report.isValid(), is(true));
        }

        @Test
        @DisplayName("accepts a body producing List<JSON_OBJECT> where List<JSON_ELEMENT> is expected")
        void expectedListOutputWidens() {
            assertThat(Chain.validate(DataTypes.JSON_ELEMENT, List.of(DeserializeTransform.of(ROWS)), ELEMENTS).isValid(), is(true));
        }

        @Test
        @DisplayName("still refuses a body producing JSON_ELEMENT where JSON_OBJECT is expected")
        void expectedOutputDoesNotNarrow() {
            assertThat(Chain.validate(DataTypes.RAW_JSON, List.of(ParseJsonTransform.of()), DataTypes.JSON_OBJECT).isValid(), is(false));
        }

        @Test
        @DisplayName("lets a factory build a body over a widened element")
        void factoryBuildsWidenedBody() {
            MapTransform<JsonObject, String> map = MapTransform.of(DataTypes.JSON_OBJECT, DataTypes.STRING, List.of(StringifyTransform.of()));
            assertThat(map.inputType(), is(equalTo(ROWS)));
        }

    }

    @Nested
    @DisplayName("An operand")
    class Operand {

        @Test
        @DisplayName("producing JSON_OBJECT satisfies an expected JSON_ELEMENT")
        void operandWidens() {
            assertThat(object().build().validate(DataTypes.JSON_ELEMENT).isValid(), is(true));
        }

        @Test
        @DisplayName("producing List<JSON_OBJECT> satisfies an expected List<JSON_ELEMENT>")
        void operandListWidens() {
            assertThat(rows().validate(ELEMENTS).isValid(), is(true));
        }

        @Test
        @DisplayName("producing JSON_ELEMENT still does not satisfy an expected JSON_OBJECT")
        void operandDoesNotNarrow() {
            DataPipeline<JsonElement> parsed = DataPipeline.builder()
                .source(LiteralSource.rawJson("{}"))
                .stage(ParseJsonTransform.of())
                .build();
            assertThat(parsed.validate(DataTypes.JSON_OBJECT).isValid(), is(false));
        }

        @Test
        @DisplayName("producing List<JSON_OBJECT> loads and runs under a stage expecting List<JSON_ELEMENT>")
        void widenedOperandLoadsAndRuns() {
            DataPipeline<?> pipeline = PipelineGson.fromJson(q("[{'kind':'SOURCE_LITERAL','outputType':'RAW_JSON','value':'[3]'},"
                + "{'kind':'PARSE_JSON'},"
                + "{'kind':'TRANSFORM_JSON_DESERIALIZE','inputType':'JSON_ELEMENT','outputType':'List<JSON_ELEMENT>'},"
                + "{'kind':'TRANSFORM_CONCAT','elementType':'JSON_ELEMENT','other':[" + WIRE_ROWS + "]},"
                + "{'kind':'TRANSFORM_MAP','elementInputType':'JSON_ELEMENT','elementOutputType':'STRING',"
                + "'body':[{'kind':'TRANSFORM_JSON_STRINGIFY'}]}]"));
            assertThat(pipeline.execute(), is(equalTo(List.of("3", "{\"a\":1}", "{\"a\":2}"))));
        }

        @Test
        @DisplayName("producing List<JSON_OBJECT> is accepted by a factory expecting List<JSON_ELEMENT>")
        void factoryAcceptsWidenedOperand() {
            assertThat(ConcatTransform.of(DataTypes.JSON_ELEMENT, rows()).inputType(), is(equalTo(ELEMENTS)));
        }

    }

    @Nested
    @DisplayName("Narrowing a pipeline's output")
    class ExpectOutput {

        @Test
        @DisplayName("to a type its output widens to succeeds")
        void expectOutputWidens() {
            DataPipeline<JsonObject> pipeline = object().build();
            assertThat(pipeline.expectOutput(DataTypes.JSON_ELEMENT), is(sameInstance(pipeline)));
        }

        @Test
        @DisplayName("lets an embed declared JSON_ELEMENT run a saved pipeline producing JSON_OBJECT")
        void embedRunsWidenedPipeline() {
            DataPipeline<JsonObject> saved = object().build();
            DataPipelineResolver resolver = new DataPipelineResolver() {

                @Override
                public @NotNull Optional<DataPipeline<?>> resolve(@NotNull String id) {
                    return Optional.of(saved);
                }

                @Override
                public @Nullable String idOf(@NotNull DataPipeline<?> pipeline) {
                    return null;
                }

            };
            DataPipeline<String> pipeline = DataPipeline.builder()
                .source(EmbedSource.of("saved", DataTypes.JSON_ELEMENT))
                .stage(StringifyTransform.of())
                .build();
            assertThat(pipeline.execute(PipelineContext.builder().withResolver(resolver).build()), is(equalTo("{\"a\":1}")));
        }

    }

}
