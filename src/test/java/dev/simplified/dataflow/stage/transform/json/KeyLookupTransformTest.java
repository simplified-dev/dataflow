package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.SourceStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.source.UrlSource;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link KeyLookupTransform}: resolution and its misses, key comparison, copying, use
 * inside a Map body and an ObjectBuild output, the table's single read and index per context,
 * factory rejection and the wire.
 */
class KeyLookupTransformTest {

    private static final @NotNull String TABLE = "[{'name':'Stone','id':'STONE'},{'name':'Gem','id':'GEM'}]";

    /**
     * Turns single-quoted JSON into JSON, so a row literal needs no escaping.
     *
     * @param json the single-quoted JSON
     * @return the JSON
     */
    private static @NotNull String q(@NotNull String json) {
        return json.replace('\'', '"');
    }

    private static @NotNull DataPipeline<List<JsonObject>> table(@NotNull SourceStage<String> source) {
        return DataPipeline.builder()
            .source(source)
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .build();
    }

    private static @NotNull DataPipeline<List<JsonObject>> table(@NotNull String json) {
        return table(LiteralSource.rawJson(q(json)));
    }

    private static @Nullable JsonElement lookup(@NotNull String json, @Nullable String key) {
        return KeyLookupTransform.of("name", "id", table(json)).execute(PipelineContext.defaults(), key);
    }

    private static @NotNull MapTransform<String, JsonElement> mapOver(@NotNull KeyLookupTransform lookup) {
        return MapTransform.of(DataTypes.STRING, DataTypes.JSON_ELEMENT, List.of(lookup));
    }

    @SuppressWarnings("unchecked")
    private static @NotNull String text(@Nullable Object values) {
        if (values == null) return "null";
        JsonArray array = new JsonArray();
        ((List<JsonElement>) values).forEach(array::add);
        return array.toString();
    }

    private static @NotNull PipelineContext tracing(@NotNull Object watched, @NotNull AtomicInteger runs) {
        return PipelineContext.builder()
            .withTrace((stage, output) -> {
                if (stage == watched) runs.incrementAndGet();
            })
            .build();
    }

    @Test
    @DisplayName("A key resolves to the matching row's value field")
    void keyResolves() {
        assertThat(lookup(TABLE, "Gem"), is(equalTo(JsonParser.parseString("\"GEM\""))));
    }

    @Test
    @DisplayName("A key no row carries yields null")
    void missingKeyYieldsNull() {
        assertThat(lookup(TABLE, "Ore"), is(nullValue()));
    }

    @Test
    @DisplayName("A null element yields null")
    void nullElementYieldsNull() {
        assertThat(lookup(TABLE, null), is(nullValue()));
    }

    @Test
    @DisplayName("A null operand output yields null")
    void nullOperandYieldsNull() {
        assertThat(lookup("{'name':'Stone','id':'STONE'}", "Stone"), is(nullValue()));
    }

    @Test
    @DisplayName("When two rows carry one key, the first is the one matched")
    void firstRowWins() {
        assertThat(lookup("[{'name':'Stone','id':'A'},{'name':'Stone','id':'B'}]", "Stone"), is(equalTo(JsonParser.parseString("\"A\""))));
    }

    @Test
    @DisplayName("Keys compare by string form, so text matches a numeric key")
    void textMatchesNumericKey() {
        assertThat(lookup("[{'name':5,'id':'FIVE'}]", "5"), is(equalTo(JsonParser.parseString("\"FIVE\""))));
    }

    @Test
    @DisplayName("A row whose key is JSON null is never matched, not even by the text null")
    void nullKeyNeverMatched() {
        assertThat(lookup("[{'name':null,'id':'X'}]", "null"), is(nullValue()));
    }

    @Test
    @DisplayName("A row whose value field is JSON null yields null")
    void jsonNullValueYieldsNull() {
        assertThat(lookup("[{'name':'Stone','id':null}]", "Stone"), is(nullValue()));
    }

    @Test
    @DisplayName("A row without the value field yields null")
    void absentValueYieldsNull() {
        assertThat(lookup("[{'name':'Stone'}]", "Stone"), is(nullValue()));
    }

    @Test
    @DisplayName("A structured value is returned whole")
    void structuredValueReturned() {
        assertThat(lookup("[{'name':'Stone','id':{'a':[1,2]}}]", "Stone"), is(equalTo(JsonParser.parseString(q("{'a':[1,2]}")))));
    }

    @Test
    @DisplayName("The value is a copy, so changing it leaves the table as read")
    void valueIsCopy() {
        KeyLookupTransform stage = KeyLookupTransform.of("name", "id", table("[{'name':'Stone','id':{'a':1}}]"));
        PipelineContext ctx = PipelineContext.defaults();
        stage.execute(ctx, "Stone").getAsJsonObject().addProperty("b", 2);
        assertThat(stage.execute(ctx, "Stone"), is(equalTo(JsonParser.parseString(q("{'a':1}")))));
    }

    @Test
    @DisplayName("A Map body drops an element whose key no row carries")
    void mapBodyDropsMiss() {
        DataPipeline<List<JsonElement>> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings("Stone", "Ore", "Gem"))
            .stage(mapOver(KeyLookupTransform.of("name", "id", table(TABLE))))
            .build();
        assertThat(text(pipeline.execute()), is(equalTo(q("['STONE','GEM']"))));
    }

    private static @NotNull ObjectBuildTransform<JsonObject> build(@NotNull KeyLookupTransform lookup) {
        return ObjectBuildTransform.over(DataTypes.JSON_OBJECT)
            .output("item", DataTypes.JSON_ELEMENT, c -> c
                .stage(FieldTransform.of("stone"))
                .stage(AsStringTransform.of())
                .stage(lookup))
            .build();
    }

    @Test
    @DisplayName("An ObjectBuild output takes the looked-up value")
    void objectBuildTakesValue() {
        JsonObject row = JsonParser.parseString(q("{'stone':'Gem'}")).getAsJsonObject();
        assertThat(build(KeyLookupTransform.of("name", "id", table(TABLE))).execute(PipelineContext.defaults(), row),
            is(equalTo(JsonParser.parseString(q("{'item':'GEM'}")))));
    }

    @Test
    @DisplayName("An ObjectBuild output is omitted when its key no row carries")
    void objectBuildOmitsMiss() {
        JsonObject row = JsonParser.parseString(q("{'stone':'Ore'}")).getAsJsonObject();
        assertThat(build(KeyLookupTransform.of("name", "id", table(TABLE))).execute(PipelineContext.defaults(), row),
            is(equalTo(new JsonObject())));
    }

    @Test
    @DisplayName("A Map body reads the table once for all its elements")
    void mapBodyReadsTableOnce() {
        LiteralSource<String> source = LiteralSource.rawJson(q(TABLE));
        MapTransform<String, JsonElement> map = mapOver(KeyLookupTransform.of("name", "id", table(source)));
        AtomicInteger runs = new AtomicInteger();
        map.execute(tracing(source, runs), List.of("Stone", "Gem", "Ore"));
        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A tracer sees only registered stages, and no step for the indexing")
    void tracerSeesOnlyRegisteredStages() {
        MapTransform<String, JsonElement> map = mapOver(KeyLookupTransform.of("name", "id", table(TABLE)));
        List<String> unregistered = new ArrayList<>();
        PipelineContext ctx = PipelineContext.builder()
            .withTrace((stage, output) -> {
                if (!stage.getClass().isAnnotationPresent(StageSpec.class)) unregistered.add(stage.getClass().getName());
            })
            .build();
        map.execute(ctx, List.of("Stone", "Gem", "Ore"));
        assertThat(unregistered, is(empty()));
    }

    @Test
    @DisplayName("The table's index is built once per context")
    void indexBuiltOncePerContext() {
        RowKeys.Index index = RowKeys.index(table(TABLE), "name");
        PipelineContext ctx = PipelineContext.defaults();
        assertThat(index.read(ctx), is(sameInstance(index.read(ctx))));
    }

    @Test
    @DisplayName("Each context builds its own index")
    void eachContextBuildsIndex() {
        RowKeys.Index index = RowKeys.index(table(TABLE), "name");
        assertThat(index.read(PipelineContext.defaults()), is(not(sameInstance(index.read(PipelineContext.defaults())))));
    }

    @Test
    @DisplayName("A context made by mutate() builds its own index")
    void mutatedContextBuildsIndex() {
        RowKeys.Index index = RowKeys.index(table(TABLE), "name");
        PipelineContext ctx = PipelineContext.defaults();
        assertThat(index.read(ctx.mutate().build()), is(not(sameInstance(index.read(ctx)))));
    }

    @Test
    @DisplayName("The index is held by the context, under the index instance")
    void indexHeldByContext() {
        RowKeys.Index index = RowKeys.index(table(TABLE), "name");
        PipelineContext ctx = PipelineContext.defaults();
        Map<String, JsonObject> built = index.read(ctx);
        Map<String, JsonObject> held = ctx.derive(index, () -> null);
        assertThat(held, is(sameInstance(built)));
    }

    @Test
    @DisplayName("Each context reads the table afresh")
    void eachContextReadsTable() {
        LiteralSource<String> source = LiteralSource.rawJson(q(TABLE));
        KeyLookupTransform stage = KeyLookupTransform.of("name", "id", table(source));
        AtomicInteger runs = new AtomicInteger();
        stage.execute(tracing(source, runs), "Stone");
        stage.execute(tracing(source, runs), "Stone");
        assertThat(runs.get(), is(2));
    }

    @Test
    @DisplayName("of rejects a table that does not produce List<JSON_OBJECT>")
    void rejectsWrongTableType() {
        DataPipeline<List<String>> wrong = DataPipeline.builder().source(LiteralListSource.strings("a")).build();
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> KeyLookupTransform.of("name", "id", wrong));
        assertThat(thrown.getMessage(), startsWith("Invalid KeyLookupTransform operand"));
    }

    private static @NotNull DataPipeline<?> wirePipeline() {
        return DataPipeline.builder()
            .source(LiteralListSource.strings("Gem", "Ore", "Stone"))
            .stage(mapOver(KeyLookupTransform.of("name", "id", table(TABLE))))
            .build();
    }

    @Test
    @DisplayName("A lookup inside a Map body round-trips on the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(wirePipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A lookup inside a Map body round-trips on the wire to the same output")
    void wireRoundTripExecutes() {
        assertThat(text(PipelineGson.fromJson(PipelineGson.toJson(wirePipeline())).execute()), is(equalTo(q("['GEM','STONE']"))));
    }

    @Test
    @DisplayName("A table of the wrong output type fails the load with the factory's message")
    void wrongTableTypeRejectedAtLoad() {
        String json = q("[{'kind':'SOURCE_LITERAL','outputType':'STRING','value':'Gem'},"
            + "{'kind':'TRANSFORM_KEY_LOOKUP','keyField':'name','valueField':'id',"
            + "'table':[{'kind':'SOURCE_LITERAL_LIST','elementType':'STRING','value':'[\\'Gem\\']'}]}]");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        assertThat(root.getMessage(), startsWith("Invalid KeyLookupTransform operand"));
    }

    @Test
    @DisplayName("A table served over HTTP is fetched once for a Map body over three keys")
    void httpTableFetchedOnce() throws IOException {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/rows", exchange -> {
            hits.incrementAndGet();
            byte[] body = q(TABLE).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();

        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/rows";
            MapTransform<String, JsonElement> map = mapOver(KeyLookupTransform.of("name", "id", table(UrlSource.rawJson(url))));
            map.execute(PipelineContext.defaults(), List.of("Stone", "Gem", "Ore"));
        } finally {
            server.stop(0);
        }

        assertThat(hits.get(), is(1));
    }

}
