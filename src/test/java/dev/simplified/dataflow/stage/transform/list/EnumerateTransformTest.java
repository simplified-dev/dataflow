package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EnumerateTransformTest {

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull String json(@Nullable Object value) {
        return PipelineGson.gson().toJson(value);
    }

    private <T> @Nullable List<JsonObject> enumerate(@NotNull EnumerateTransform<T> stage, @Nullable List<T> input) {
        return stage.execute(this.ctx, input);
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralListSource.strings("a", "b", "c"))
            .stage(EnumerateTransform.of(DataTypes.STRING, 1, "tier", null))
            .build();
    }

    @Test
    @DisplayName("Each element becomes an index and value object, in order, from 0")
    void enumeratesFromZero() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, null, null), List.of("a", "b"));
        assertThat(json(result), is(equalTo("[{\"index\":0,\"value\":\"a\"},{\"index\":1,\"value\":\"b\"}]")));
    }

    @Test
    @DisplayName("start sets the first index")
    void startOffsetsIndex() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.STRING, 1, null, null), List.of("a", "b"));
        assertThat(result.getLast().get("index").getAsInt(), is(2));
    }

    @Test
    @DisplayName("indexKey and valueKey name the object's keys")
    void keysAreConfigurable() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, "tier", "name"), List.of("a"));
        assertThat(json(result), is(equalTo("[{\"tier\":0,\"name\":\"a\"}]")));
    }

    @Test
    @DisplayName("A null element is dropped and still uses up its number")
    void nullElementUsesItsNumber() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, null, null), Arrays.asList("a", null, "c"));
        assertThat(json(result), is(equalTo("[{\"index\":0,\"value\":\"a\"},{\"index\":2,\"value\":\"c\"}]")));
    }

    @Test
    @DisplayName("A JSON null element is dropped and still uses up its number")
    void jsonNullElementUsesItsNumber() {
        List<JsonElement> input = List.of(JsonParser.parseString("1"), JsonNull.INSTANCE, JsonParser.parseString("3"));
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.JSON_ELEMENT, null, null, null), input);
        assertThat(json(result), is(equalTo("[{\"index\":0,\"value\":1},{\"index\":2,\"value\":3}]")));
    }

    @Test
    @DisplayName("A numeric element is written as a JSON number")
    void numericElementWrittenAsNumber() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.INT, null, null, null), List.of(5, 7));
        assertThat(json(result), is(equalTo("[{\"index\":0,\"value\":5},{\"index\":1,\"value\":7}]")));
    }

    @Test
    @DisplayName("A JSON element is copied, so changing the output leaves the input unchanged")
    void jsonElementCopied() {
        JsonObject row = JsonParser.parseString("{\"k\":1}").getAsJsonObject();
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.JSON_OBJECT, null, null, null), List.of(row));
        result.getFirst().getAsJsonObject("value").addProperty("k", 2);
        assertThat(row.get("k").getAsInt(), is(1));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, null, null), null), is(nullValue()));
    }

    @Test
    @DisplayName("An empty list stays empty")
    void emptyStaysEmpty() {
        assertThat(this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, null, null), List.of()), is(empty()));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<JsonObject> result = this.enumerate(EnumerateTransform.of(DataTypes.STRING, null, null, null), List.of("a"));
        assertThrows(UnsupportedOperationException.class, () -> result.add(new JsonObject()));
    }

    @Test
    @DisplayName("The output type is List<JSON_OBJECT>")
    void outputTypeIsObjectList() {
        assertThat(EnumerateTransform.of(DataTypes.INT, null, null, null).outputType().label(), is(equalTo("List<JSON_OBJECT>")));
    }

    @Test
    @DisplayName("of rejects DOM_NODE, which Gson cannot write")
    void rejectsDomNode() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> EnumerateTransform.of(DataTypes.DOM_NODE, null, null, null));
        assertThat(thrown.getMessage(), startsWith("Invalid EnumerateTransform elementType"));
    }

    @Test
    @DisplayName("of rejects a list of DOM_NODE")
    void rejectsDomNodeList() {
        assertThrows(IllegalArgumentException.class, () -> EnumerateTransform.of(DataType.list(DataTypes.DOM_NODE), null, null, null));
    }

    @Test
    @DisplayName("of rejects NONE, which carries no value")
    void rejectsNone() {
        assertThrows(IllegalArgumentException.class, () -> EnumerateTransform.of(DataTypes.NONE, null, null, null));
    }

    @Test
    @DisplayName("of rejects an index key equal to the value key")
    void rejectsSameKeys() {
        assertThrows(IllegalArgumentException.class, () -> EnumerateTransform.of(DataTypes.STRING, null, "value", null));
    }

    @Test
    @DisplayName("Absent optional slots stay absent from the config")
    void absentSlotsStayAbsent() {
        EnumerateTransform<String> stage = EnumerateTransform.of(DataTypes.STRING, null, null, null);
        assertThat(stage.config().has("start") || stage.config().has("indexKey") || stage.config().has("valueKey"), is(false));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The wire form carries only the slots that were set")
    void wireFormCarriesSetSlots() {
        assertThat(PipelineGson.toJson(pipeline()), containsString(
            "{\"kind\":\"TRANSFORM_ENUMERATE\",\"elementType\":\"STRING\",\"start\":1,\"indexKey\":\"tier\"}"
        ));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> pipeline = pipeline();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A wire DOM_NODE element type fails at load")
    void wireDomNodeFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"RAW_HTML\",\"value\":\"<p>a</p>\"},{\"kind\":\"PARSE_HTML\"},"
            + "{\"kind\":\"TRANSFORM_CSS_SELECT\",\"selector\":\"p\"},{\"kind\":\"TRANSFORM_ENUMERATE\",\"elementType\":\"DOM_NODE\"}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid EnumerateTransform elementType"));
    }

}
