package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.FirstCollect;
import dev.simplified.dataflow.stage.transform.dom.CssSelectTransform;
import dev.simplified.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.simplified.dataflow.stage.transform.dom.TextTransform;
import dev.simplified.dataflow.stage.transform.json.AsStringTransform;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.FieldTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BroadcastTransformTest {

    private static final @NotNull String KEYWORD = "{\"symbol\":\"S\",\"usages\":[\"a\",\"b\",\"c\"]}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull String json(@Nullable Object value) {
        return PipelineGson.gson().toJson(value);
    }

    private static @NotNull JsonObject object(@NotNull String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private static @NotNull List<Stage<?, ?>> usages() {
        return List.of(FieldTransform.of("usages"), DeserializeTransform.of(DataType.list(DataTypes.STRING)));
    }

    private static @NotNull BroadcastTransform<JsonObject, String, String> keywords() {
        return BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            List.of(FieldTransform.of("symbol"), AsStringTransform.of()), usages(),
            "symbol", "usage"
        );
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(KEYWORD))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataTypes.JSON_OBJECT))
            .stage(keywords())
            .build();
    }

    @Test
    @DisplayName("There is one output per child, in order, each carrying the parent")
    void carriesParentOntoEachChild() {
        List<JsonObject> result = keywords().execute(this.ctx, object(KEYWORD));
        assertThat(json(result), is(equalTo(
            "[{\"symbol\":\"S\",\"usage\":\"a\"},{\"symbol\":\"S\",\"usage\":\"b\"},{\"symbol\":\"S\",\"usage\":\"c\"}]"
        )));
    }

    @Test
    @DisplayName("A null parent omits the parent key from every output")
    void nullParentOmitsKey() {
        List<JsonObject> result = keywords().execute(this.ctx, object("{\"usages\":[\"a\",\"b\"]}"));
        assertThat(json(result), is(equalTo("[{\"usage\":\"a\"},{\"usage\":\"b\"}]")));
    }

    @Test
    @DisplayName("A JSON null parent omits the parent key from every output")
    void jsonNullParentOmitsKey() {
        BroadcastTransform<JsonObject, JsonElement, String> stage = BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.JSON_ELEMENT, DataTypes.STRING,
            List.of(FieldTransform.of("symbol")), usages(), "symbol", "usage"
        );
        assertThat(json(stage.execute(this.ctx, object("{\"symbol\":null,\"usages\":[\"a\"]}"))), is(equalTo("[{\"usage\":\"a\"}]")));
    }

    @Test
    @DisplayName("A null children list rejects with null")
    void nullChildrenRejects() {
        assertThat(keywords().execute(this.ctx, object("{\"symbol\":\"S\"}")), is(nullValue()));
    }

    @Test
    @DisplayName("A null child is dropped")
    void nullChildDropped() {
        BroadcastTransform<List<String>, Integer, String> stage = BroadcastTransform.of(
            DataType.list(DataTypes.STRING), DataTypes.INT, DataTypes.STRING,
            List.of(SizeTransform.of(DataTypes.STRING)), List.of(ReverseTransform.of(DataTypes.STRING)), "n", "c"
        );
        assertThat(json(stage.execute(this.ctx, Arrays.asList("a", null, "b"))), is(equalTo("[{\"n\":3,\"c\":\"b\"},{\"n\":3,\"c\":\"a\"}]")));
    }

    @Test
    @DisplayName("An empty children list gives an empty list")
    void emptyChildrenGiveEmpty() {
        assertThat(keywords().execute(this.ctx, object("{\"symbol\":\"S\",\"usages\":[]}")), is(empty()));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(keywords().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<JsonObject> result = keywords().execute(this.ctx, object(KEYWORD));
        assertThrows(UnsupportedOperationException.class, () -> result.add(new JsonObject()));
    }

    @Test
    @DisplayName("Each output holds its own copy of the parent")
    void parentCopiedPerOutput() {
        BroadcastTransform<JsonObject, JsonElement, String> stage = BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.JSON_ELEMENT, DataTypes.STRING,
            List.of(FieldTransform.of("meta")), usages(), "meta", "usage"
        );
        JsonObject input = object("{\"meta\":{\"k\":1},\"usages\":[\"a\",\"b\"]}");
        List<JsonObject> result = stage.execute(this.ctx, input);
        result.getFirst().getAsJsonObject("meta").addProperty("k", 2);
        assertThat(json(result.getLast().get("meta")), is(equalTo("{\"k\":1}")));
    }

    @Test
    @DisplayName("Changing an output leaves the input unchanged")
    void inputUnchanged() {
        BroadcastTransform<JsonObject, JsonElement, String> stage = BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.JSON_ELEMENT, DataTypes.STRING,
            List.of(FieldTransform.of("meta")), usages(), "meta", "usage"
        );
        JsonObject input = object("{\"meta\":{\"k\":1},\"usages\":[\"a\"]}");
        stage.execute(this.ctx, input).getFirst().getAsJsonObject("meta").addProperty("k", 2);
        assertThat(json(input), is(equalTo("{\"meta\":{\"k\":1},\"usages\":[\"a\"]}")));
    }

    @Test
    @DisplayName("A table row's header is carried onto each of its cells")
    void domRowHeaderOntoCells() {
        BroadcastTransform<Element, String, String> stage = BroadcastTransform.of(
            DataTypes.DOM_NODE, DataTypes.STRING, DataTypes.STRING,
            List.of(CssSelectTransform.of("th"), FirstCollect.of(DataTypes.DOM_NODE), TextTransform.of()),
            List.of(CssSelectTransform.of("td"), MapTransform.of(DataTypes.DOM_NODE, DataTypes.STRING, List.of(TextTransform.of()))),
            "symbol", "usage"
        );
        Element row = ParseHtmlTransform.of().execute(this.ctx, "<table><tr><th>S</th><td>a</td><td>b</td></tr></table>");
        assertThat(json(stage.execute(this.ctx, row)), is(equalTo("[{\"symbol\":\"S\",\"usage\":\"a\"},{\"symbol\":\"S\",\"usage\":\"b\"}]")));
    }

    @Test
    @DisplayName("of rejects a parent body that does not yield the parent type")
    void rejectsMistypedParent() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.INT, DataTypes.STRING,
            List.of(FieldTransform.of("symbol"), AsStringTransform.of()), usages(), "symbol", "usage"
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid BroadcastTransform parent body"));
    }

    @Test
    @DisplayName("of rejects a children body that does not consume the input type")
    void rejectsMistypedChildren() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            List.of(FieldTransform.of("symbol"), AsStringTransform.of()), List.of(UpperCaseTransform.of()), "symbol", "usage"
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid BroadcastTransform children body"));
    }

    @Test
    @DisplayName("of rejects an empty parent body")
    void rejectsEmptyParent() {
        assertThrows(IllegalArgumentException.class, () -> BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.JSON_OBJECT, DataTypes.STRING, List.of(), usages(), "symbol", "usage"
        ));
    }

    @Test
    @DisplayName("of rejects a DOM_NODE parent type, which Gson cannot write")
    void rejectsDomNodeParent() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> BroadcastTransform.of(
            DataTypes.DOM_NODE, DataTypes.DOM_NODE, DataTypes.STRING,
            List.of(CssSelectTransform.of("th"), FirstCollect.of(DataTypes.DOM_NODE)),
            List.of(CssSelectTransform.of("td"), MapTransform.of(DataTypes.DOM_NODE, DataTypes.STRING, List.of(TextTransform.of()))),
            "symbol", "usage"
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid BroadcastTransform parentType"));
    }

    @Test
    @DisplayName("of rejects the same key for the parent and the children")
    void rejectsSameKeys() {
        assertThrows(IllegalArgumentException.class, () -> BroadcastTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            List.of(FieldTransform.of("symbol"), AsStringTransform.of()), usages(), "k", "k"
        ));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> pipeline = pipeline();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A wire children body that does not yield the children list fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"s:a,b\"},"
            + "{\"kind\":\"TRANSFORM_BROADCAST\",\"inputType\":\"STRING\",\"parentType\":\"STRING\",\"childType\":\"INT\","
            + "\"parent\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}],\"children\":[{\"kind\":\"TRANSFORM_SPLIT\",\"regex\":\",\"}],"
            + "\"parentKey\":\"p\",\"childKey\":\"c\"}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid BroadcastTransform children body"));
    }

}
