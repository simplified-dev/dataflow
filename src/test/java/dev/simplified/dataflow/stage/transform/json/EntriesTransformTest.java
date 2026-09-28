package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.JsonObjectFromEntriesCollect;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntriesTransformTest {

    private static final @NotNull String SKILLS = """
        {"MINING":{"name":"Mining","maxLevel":60},"COMBAT":{"name":"Combat","maxLevel":60},"ALCHEMY":{"name":"Alchemy","maxLevel":50}}""";

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull JsonObject object(@NotNull String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private @NotNull List<JsonObject> entries(@NotNull EntriesTransform stage, @NotNull String json) {
        return stage.execute(this.ctx, object(json));
    }

    private static @NotNull DataPipeline<List<JsonObject>> pipeline(@NotNull EntriesTransform stage) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(SKILLS))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataTypes.JSON_OBJECT))
            .stage(stage)
            .build();
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(EntriesTransform.of(null, null, null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Each entry becomes a {key, value} object with the default field names")
    void defaultFields() {
        assertThat(entries(EntriesTransform.of(null, null, null), "{\"a\":1}").getFirst(), is(equalTo(object("{\"key\":\"a\",\"value\":1}"))));
    }

    @Test
    @DisplayName("Entries come out in the object's document order")
    void documentOrder() {
        List<String> keys = entries(EntriesTransform.of(null, null, null), SKILLS).stream()
            .map(row -> row.get("key").getAsString())
            .toList();
        assertThat(keys, contains("MINING", "COMBAT", "ALCHEMY"));
    }

    @Test
    @DisplayName("The key is a JSON string")
    void keyIsString() {
        assertThat(entries(EntriesTransform.of(null, null, null), "{\"7\":true}").getFirst().get("key").getAsJsonPrimitive().isString(), is(true));
    }

    @Test
    @DisplayName("A JSON null value stays a JSON null")
    void nullValueStays() {
        assertThat(entries(EntriesTransform.of(null, null, null), "{\"a\":null}").getFirst().get("value"), is(equalTo(JsonNull.INSTANCE)));
    }

    @Test
    @DisplayName("An empty object yields an empty list")
    void emptyObject() {
        assertThat(entries(EntriesTransform.of(null, null, null), "{}"), is(empty()));
    }

    @Test
    @DisplayName("Configured field names replace key and value")
    void configuredFields() {
        assertThat(entries(EntriesTransform.of("id", "data", null), "{\"a\":[1]}").getFirst(), is(equalTo(object("{\"id\":\"a\",\"data\":[1]}"))));
    }

    @Test
    @DisplayName("Inline copies an object value's fields after the key")
    void inlineCopiesFields() {
        assertThat(
            entries(EntriesTransform.of("id", null, true), SKILLS).getFirst(),
            is(equalTo(object("{\"id\":\"MINING\",\"name\":\"Mining\",\"maxLevel\":60}")))
        );
    }

    @Test
    @DisplayName("Inline keeps the key first and the value's field order after it")
    void inlineOrder() {
        List<String> fields = List.copyOf(entries(EntriesTransform.of("id", null, true), SKILLS).getFirst().keySet());
        assertThat(fields, contains("id", "name", "maxLevel"));
    }

    @Test
    @DisplayName("Inline lets the key overwrite a value field of the same name")
    void inlineKeyWins() {
        assertThat(
            entries(EntriesTransform.of(null, null, true), "{\"a\":{\"key\":\"stale\",\"x\":1}}").getFirst(),
            is(equalTo(object("{\"key\":\"a\",\"x\":1}")))
        );
    }

    @Test
    @DisplayName("Inline falls back to the value field for a value that is not an object")
    void inlineFallsBack() {
        assertThat(entries(EntriesTransform.of(null, null, true), "{\"a\":[1,2]}").getFirst(), is(equalTo(object("{\"key\":\"a\",\"value\":[1,2]}"))));
    }

    @Test
    @DisplayName("Inline false nests an object value under the value field")
    void inlineFalseNests() {
        assertThat(entries(EntriesTransform.of(null, null, false), "{\"a\":{\"x\":1}}").getFirst(), is(equalTo(object("{\"key\":\"a\",\"value\":{\"x\":1}}"))));
    }

    @Test
    @DisplayName("The input object is left unchanged")
    void inputUnchanged() {
        JsonObject input = object(SKILLS);
        EntriesTransform.of("name", null, true).execute(this.ctx, input);
        assertThat(input, is(equalTo(object(SKILLS))));
    }

    @Test
    @DisplayName("The output list is unmodifiable")
    void outputUnmodifiable() {
        List<JsonObject> rows = entries(EntriesTransform.of(null, null, null), "{\"a\":1}");
        assertThrows(UnsupportedOperationException.class, () -> rows.add(new JsonObject()));
    }

    @Test
    @DisplayName("Collecting the entries back rebuilds the object")
    void inverseOfFromEntries() {
        JsonObject rebuilt = JsonObjectFromEntriesCollect.of().execute(this.ctx, entries(EntriesTransform.of(null, null, null), SKILLS));
        assertThat(rebuilt, is(equalTo(object(SKILLS))));
    }

    @Test
    @DisplayName("Collecting the entries back drops a null entry")
    void roundTripDropsNull() {
        JsonObject rebuilt = JsonObjectFromEntriesCollect.of().execute(this.ctx, entries(EntriesTransform.of(null, null, null), "{\"a\":1,\"b\":null}"));
        assertThat(rebuilt, is(equalTo(object("{\"a\":1}"))));
    }

    @Test
    @DisplayName("of rejects a key field and a value field of the same name")
    void rejectsSameNames() {
        assertThrows(IllegalArgumentException.class, () -> EntriesTransform.of("x", "x", null));
    }

    @Test
    @DisplayName("of rejects a key field named like the default value field")
    void rejectsKeyNamedValue() {
        assertThrows(IllegalArgumentException.class, () -> EntriesTransform.of("value", null, null));
    }

    @Test
    @DisplayName("Absent fields are left out of the configuration")
    void absentFieldsOmitted() {
        assertThat(EntriesTransform.of(null, null, null).config().has("keyField"), is(false));
    }

    @Test
    @DisplayName("A pipeline with default fields round-trips to the same JSON")
    void wireRoundTripDefaults() {
        String first = PipelineGson.toJson(pipeline(EntriesTransform.of(null, null, null)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with every field set round-trips to the same JSON")
    void wireRoundTripConfigured() {
        String first = PipelineGson.toJson(pipeline(EntriesTransform.of("id", "data", true)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The wire form carries the configured fields")
    void wireFormCarriesFields() {
        assertThat(
            PipelineGson.toJson(pipeline(EntriesTransform.of("id", "data", true))),
            containsString("{\"kind\":\"TRANSFORM_JSON_ENTRIES\",\"keyField\":\"id\",\"valueField\":\"data\",\"inline\":true}")
        );
    }

    @Test
    @DisplayName("A pipeline round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<List<JsonObject>> pipeline = pipeline(EntriesTransform.of("id", null, true));
        Object rebuilt = PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute();
        assertThat(rebuilt, is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("The round-tripped pipeline emits one row per entry")
    void wireRoundTripRows() {
        Object rebuilt = PipelineGson.fromJson(PipelineGson.toJson(pipeline(EntriesTransform.of(null, null, null)))).execute();
        assertThat(((List<?>) rebuilt).size(), is(equalTo(3)));
    }

    @Test
    @DisplayName("Summary names the key and value fields")
    void summaryNamesFields() {
        assertThat(EntriesTransform.of("id", null, null).summary(), is(equalTo("Entries as {id, value}")));
    }

}
