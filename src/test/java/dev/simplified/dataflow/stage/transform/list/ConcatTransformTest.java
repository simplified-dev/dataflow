package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.SplitTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ConcatTransform}: order, no de-duplication, empty and null sides, chained
 * documents, the operand's single evaluation, factory rejections and the wire.
 */
class ConcatTransformTest {

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull DataPipeline<List<String>> strings(@NotNull String... values) {
        return DataPipeline.builder().source(LiteralListSource.strings(values)).build();
    }

    private static @NotNull DataPipeline<List<JsonObject>> rows(@NotNull String json) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(json))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .build();
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    @Test
    @DisplayName("The operand's elements follow the input's, each in order")
    void appendsInOrder() {
        assertThat(ConcatTransform.of(DataTypes.STRING, strings("c", "d")).execute(this.ctx, List.of("a", "b")),
            contains("a", "b", "c", "d"));
    }

    @Test
    @DisplayName("Nothing is de-duplicated")
    void keepsDuplicates() {
        assertThat(ConcatTransform.of(DataTypes.STRING, strings("a", "b")).execute(this.ctx, List.of("a")),
            contains("a", "a", "b"));
    }

    @Test
    @DisplayName("An empty input yields the operand's elements")
    void emptyInputYieldsOperand() {
        assertThat(ConcatTransform.of(DataTypes.STRING, strings("x")).execute(this.ctx, List.of()), contains("x"));
    }

    @Test
    @DisplayName("An empty operand yields the input's elements")
    void emptyOperandYieldsInput() {
        assertThat(ConcatTransform.of(DataTypes.STRING, strings()).execute(this.ctx, List.of("x")), contains("x"));
    }

    @Test
    @DisplayName("A null input rejects with null")
    void nullInputRejects() {
        assertThat(ConcatTransform.of(DataTypes.STRING, strings("x")).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A null operand output rejects with null")
    void nullOperandRejects() {
        DataPipeline<List<String>> rejecting = DataPipeline.builder()
            .source(LiteralSource.text("abc"))
            .stage(RegexExtractTransform.of("\\d+"))
            .stage(SplitTransform.of(","))
            .build();
        assertThat(ConcatTransform.of(DataTypes.STRING, rejecting).execute(this.ctx, List.of("x")), is(nullValue()));
    }

    @Test
    @DisplayName("Two stages append two documents in order")
    void chainedStagesAppendInOrder() {
        DataPipeline<List<String>> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings("a"))
            .stage(ConcatTransform.of(DataTypes.STRING, strings("b")))
            .stage(ConcatTransform.of(DataTypes.STRING, strings("c")))
            .build();
        assertThat(pipeline.execute(), contains("a", "b", "c"));
    }

    @Test
    @DisplayName("Rows of two JSON documents concatenate")
    void jsonRowsConcatenate() {
        DataPipeline<List<JsonObject>> pipeline = DataPipeline.builder()
            .source(LiteralSource.rawJson("[{\"id\":\"a\"}]"))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .stage(ConcatTransform.of(DataTypes.JSON_OBJECT, rows("[{\"id\":\"b\"}]")))
            .build();
        JsonArray array = new JsonArray();
        pipeline.execute().forEach(array::add);
        assertThat(array.toString(), is(equalTo("[{\"id\":\"a\"},{\"id\":\"b\"}]")));
    }

    @Test
    @DisplayName("The operand is read once per context however many times the stage runs")
    void operandReadOncePerContext() {
        LiteralListSource<String> source = LiteralListSource.strings("x");
        ConcatTransform<String> stage = ConcatTransform.of(DataTypes.STRING, DataPipeline.builder().source(source).build());
        AtomicInteger runs = new AtomicInteger();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((traced, output) -> {
                if (traced == source) runs.incrementAndGet();
            })
            .build();

        stage.execute(counting, List.of("a"));
        stage.execute(counting, List.of("b"));

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("The output list is unmodifiable")
    void outputUnmodifiable() {
        List<String> result = ConcatTransform.of(DataTypes.STRING, strings("b")).execute(this.ctx, List.of("a"));
        assertThrows(UnsupportedOperationException.class, () -> result.add("c"));
    }

    @Test
    @DisplayName("of rejects an operand producing a list of another element type")
    void rejectsOtherElementType() {
        DataPipeline<List<Integer>> ints = DataPipeline.builder().source(LiteralListSource.integers(1)).build();
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ConcatTransform.of(DataTypes.STRING, ints));
        assertThat(thrown.getMessage(), startsWith("Invalid ConcatTransform operand"));
    }

    @Test
    @DisplayName("of rejects an operand producing a single value")
    void rejectsSingleValue() {
        DataPipeline<String> single = DataPipeline.builder().source(LiteralSource.text("x")).build();
        assertThrows(IllegalArgumentException.class, () -> ConcatTransform.of(DataTypes.STRING, single));
    }

    @Test
    @DisplayName("The output type is a List of the element type")
    void outputTypeIsListOfElement() {
        ConcatTransform<String> stage = ConcatTransform.of(DataTypes.STRING, strings());
        assertThat(stage.outputType(), is(equalTo(DataType.list(DataTypes.STRING))));
    }

    private static @NotNull DataPipeline<?> wirePipeline() {
        return DataPipeline.builder()
            .source(LiteralListSource.strings("a", "b"))
            .stage(ConcatTransform.of(DataTypes.STRING, strings("b", "c")))
            .build();
    }

    @Test
    @DisplayName("A concat round-trips on the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(wirePipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A concat round-trips on the wire to the same output")
    void wireRoundTripExecutes() {
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(wirePipeline())).execute(), is(equalTo(List.of("a", "b", "b", "c"))));
    }

    @Test
    @DisplayName("An operand of the wrong element type fails the load with the factory's message")
    void wrongOperandTypeRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL_LIST\",\"elementType\":\"STRING\",\"value\":\"[\\\"a\\\"]\"},"
            + "{\"kind\":\"TRANSFORM_CONCAT\",\"elementType\":\"STRING\","
            + "\"other\":[{\"kind\":\"SOURCE_LITERAL_LIST\",\"elementType\":\"INT\",\"value\":\"[1]\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid ConcatTransform operand"));
    }

}
