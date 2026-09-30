package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GroupByTransformTest {

    private static final @NotNull String ROWS = "[{\"id\":\"b\",\"v\":1},{\"id\":\"a\",\"v\":2},{\"id\":\"b\",\"v\":3}]";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull List<JsonObject> rows(@NotNull String json) {
        List<JsonObject> rows = new ArrayList<>();

        for (JsonElement element : JsonParser.parseString(json).getAsJsonArray())
            rows.add(element.isJsonNull() ? null : element.getAsJsonObject());

        return rows;
    }

    private static @NotNull Map<String, String> table(@NotNull String... pairs) {
        Map<String, String> table = new LinkedHashMap<>();

        for (int i = 0; i < pairs.length; i += 2)
            table.put(pairs[i], pairs[i + 1]);

        return table;
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private @Nullable List<JsonObject> group(@Nullable Map<String, String> aggregates, @Nullable String json) {
        return GroupByTransform.of("id", aggregates).execute(this.ctx, json == null ? null : rows(json));
    }

    private @NotNull JsonObject only(@Nullable Map<String, String> aggregates, @NotNull String json) {
        List<JsonObject> result = this.group(aggregates, json);
        assertThat(result, hasSize(1));
        return result.getFirst();
    }

    private static @NotNull DataPipeline<?> pipeline(@NotNull String json, @Nullable Map<String, String> aggregates) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(json))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .stage(GroupByTransform.of("id", aggregates))
            .build();
    }

    @Test
    @DisplayName("Groups appear in the order their key first occurs")
    void groupsInFirstKeyOrder() {
        List<JsonObject> result = this.group(null, ROWS);
        assertThat(result.stream().map(row -> row.get("id").getAsString()).toList(), contains("b", "a"));
    }

    @Test
    @DisplayName("A field the table does not name keeps its first value")
    void unnamedFieldKeepsFirst() {
        assertThat(this.group(null, ROWS).getFirst().get("v").getAsInt(), is(1));
    }

    @Test
    @DisplayName("The first value skips a JSON null and an absent field")
    void firstSkipsNullAndAbsent() {
        JsonObject row = this.only(null, "[{\"id\":1},{\"id\":1,\"v\":null},{\"id\":1,\"v\":\"x\"},{\"id\":1,\"v\":\"y\"}]");
        assertThat(row.get("v").getAsString(), is(equalTo("x")));
    }

    @Test
    @DisplayName("A field with no non-null value in its group is omitted")
    void fieldWithNoValueOmitted() {
        JsonObject row = this.only(null, "[{\"id\":1,\"v\":null},{\"id\":1}]");
        assertThat(row.has("v"), is(false));
    }

    @Test
    @DisplayName("FIRST keeps the first non-null value")
    void firstAggregate() {
        JsonObject row = this.only(table("v", "FIRST"), "[{\"id\":1,\"v\":null},{\"id\":1,\"v\":2},{\"id\":1,\"v\":3}]");
        assertThat(row.get("v").getAsInt(), is(2));
    }

    @Test
    @DisplayName("LAST keeps the last non-null value")
    void lastAggregate() {
        JsonObject row = this.only(table("v", "LAST"), "[{\"id\":1,\"v\":1},{\"id\":1,\"v\":2},{\"id\":1,\"v\":null}]");
        assertThat(row.get("v").getAsInt(), is(2));
    }

    @Test
    @DisplayName("LIST collects the values in row order, an array value as one element")
    void listAggregate() {
        JsonObject row = this.only(table("v", "LIST"), "[{\"id\":1,\"v\":\"a\"},{\"id\":1,\"v\":[\"b\",\"c\"]},{\"id\":1,\"v\":\"a\"}]");
        assertThat(row.get("v").toString(), is(equalTo("[\"a\",[\"b\",\"c\"],\"a\"]")));
    }

    @Test
    @DisplayName("LIST skips a JSON null and an absent field")
    void listSkipsNull() {
        JsonObject row = this.only(table("v", "LIST"), "[{\"id\":1,\"v\":1},{\"id\":1,\"v\":null},{\"id\":1},{\"id\":1,\"v\":2}]");
        assertThat(row.get("v").toString(), is(equalTo("[1,2]")));
    }

    @Test
    @DisplayName("LIST over a field no row carries yields an empty array")
    void listOfNothingIsEmptyArray() {
        JsonObject row = this.only(table("tags", "LIST"), "[{\"id\":1}]");
        assertThat(row.get("tags").toString(), is(equalTo("[]")));
    }

    @Test
    @DisplayName("UNION collects distinct values in first-occurrence order, flattening arrays")
    void unionAggregate() {
        JsonObject row = this.only(table("v", "UNION"), "[{\"id\":1,\"v\":[\"a\",\"b\"]},{\"id\":1,\"v\":\"b\"},{\"id\":1,\"v\":[\"c\",\"a\"]}]");
        assertThat(row.get("v").toString(), is(equalTo("[\"a\",\"b\",\"c\"]")));
    }

    @Test
    @DisplayName("CONCAT flattens arrays in order, keeping repeats")
    void concatAggregate() {
        JsonObject row = this.only(table("v", "CONCAT"), "[{\"id\":1,\"v\":[\"a\",\"b\"]},{\"id\":1,\"v\":\"b\"},{\"id\":1,\"v\":[\"c\"]}]");
        assertThat(row.get("v").toString(), is(equalTo("[\"a\",\"b\",\"b\",\"c\"]")));
    }

    @Test
    @DisplayName("CONCAT skips a JSON null inside an array")
    void concatSkipsNullElement() {
        JsonObject row = this.only(table("v", "CONCAT"), "[{\"id\":1,\"v\":[\"a\",null]},{\"id\":1,\"v\":[\"b\"]}]");
        assertThat(row.get("v").toString(), is(equalTo("[\"a\",\"b\"]")));
    }

    @Test
    @DisplayName("MAX keeps the greatest number")
    void maxAggregate() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":3},{\"id\":1,\"v\":10},{\"id\":1,\"v\":7}]");
        assertThat(row.get("v").getAsInt(), is(10));
    }

    @Test
    @DisplayName("MIN keeps the least number")
    void minAggregate() {
        JsonObject row = this.only(table("v", "MIN"), "[{\"id\":1,\"v\":3},{\"id\":1,\"v\":10},{\"id\":1,\"v\":-7}]");
        assertThat(row.get("v").getAsInt(), is(-7));
    }

    @Test
    @DisplayName("MAX compares an integer against a decimal by value")
    void maxComparesMixedNumbers() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":2},{\"id\":1,\"v\":2.5},{\"id\":1,\"v\":-4}]");
        assertThat(row.get("v").getAsDouble(), is(2.5));
    }

    @Test
    @DisplayName("MAX keeps the earliest of equal numbers")
    void maxTieKeepsEarliest() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":2.0},{\"id\":1,\"v\":2}]");
        assertThat(row.get("v").toString(), is(equalTo("2.0")));
    }

    @Test
    @DisplayName("MAX skips a value that is not a JSON number")
    void maxSkipsNonNumbers() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":\"99\"},{\"id\":1,\"v\":4},{\"id\":1,\"v\":[100]}]");
        assertThat(row.get("v").getAsInt(), is(4));
    }

    @Test
    @DisplayName("MAX over no numbers omits the field")
    void maxOfNothingOmitted() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":\"x\"}]");
        assertThat(row.has("v"), is(false));
    }

    @Test
    @DisplayName("COUNT counts the rows of the group, whether or not they carry the field")
    void countAggregate() {
        JsonObject row = this.only(table("n", "COUNT"), "[{\"id\":1,\"n\":5},{\"id\":1},{\"id\":1}]");
        assertThat(row.get("n").getAsInt(), is(3));
    }

    @Test
    @DisplayName("MODE keeps the most frequent value")
    void modeAggregate() {
        JsonObject row = this.only(table("v", "MODE"), "[{\"id\":1,\"v\":\"a\"},{\"id\":1,\"v\":\"b\"},{\"id\":1,\"v\":\"b\"}]");
        assertThat(row.get("v").getAsString(), is(equalTo("b")));
    }

    @Test
    @DisplayName("MODE breaks a tie toward the value that occurs first")
    void modeTieKeepsEarliest() {
        JsonObject row = this.only(table("v", "MODE"), "[{\"id\":1,\"v\":\"x\"},{\"id\":1,\"v\":\"y\"},{\"id\":1,\"v\":\"y\"},{\"id\":1,\"v\":\"x\"}]");
        assertThat(row.get("v").getAsString(), is(equalTo("x")));
    }

    @Test
    @DisplayName("MODE compares object values by content")
    void modeComparesObjects() {
        JsonObject row = this.only(table("v", "MODE"), "[{\"id\":1,\"v\":{\"k\":1}},{\"id\":1,\"v\":{\"k\":2}},{\"id\":1,\"v\":{\"k\":2}}]");
        assertThat(row.get("v").toString(), is(equalTo("{\"k\":2}")));
    }

    @Test
    @DisplayName("A row with no key field is dropped")
    void rowWithoutKeyDropped() {
        List<JsonObject> result = this.group(null, "[{\"v\":1},{\"id\":\"a\",\"v\":2}]");
        assertThat(result.stream().map(JsonObject::toString).toList(), contains("{\"id\":\"a\",\"v\":2}"));
    }

    @Test
    @DisplayName("A row whose key is JSON null is dropped")
    void rowWithNullKeyDropped() {
        List<JsonObject> result = this.group(null, "[{\"id\":null,\"v\":1},{\"id\":\"a\",\"v\":2}]");
        assertThat(result.stream().map(JsonObject::toString).toList(), contains("{\"id\":\"a\",\"v\":2}"));
    }

    @Test
    @DisplayName("A null row is dropped")
    void nullRowDropped() {
        assertThat(this.group(null, "[null,{\"id\":\"a\"}]"), hasSize(1));
    }

    @Test
    @DisplayName("A string key and a number key of the same digits are different groups")
    void keysCompareByJsonValue() {
        assertThat(this.group(null, "[{\"id\":1},{\"id\":\"1\"},{\"id\":1}]"), hasSize(2));
    }

    @Test
    @DisplayName("Integer keys that differ past double precision are different groups")
    void largeIntegerKeysStayApart() {
        assertThat(this.group(null, "[{\"id\":12345678901234567},{\"id\":12345678901234568}]"), hasSize(2));
    }

    @Test
    @DisplayName("Number keys of equal value are one group whatever their written form")
    void numberKeysGroupByValue() {
        assertThat(this.group(null, "[{\"id\":1},{\"id\":1.0},{\"id\":1e0}]"), hasSize(1));
    }

    @Test
    @DisplayName("A built integer key and a parsed decimal key of equal value are one group")
    void builtAndParsedNumberKeysGroupTogether() {
        JsonObject built = new JsonObject();
        built.addProperty("id", 1);
        List<JsonObject> input = rows("[{\"id\":1.0}]");
        input.add(built);
        assertThat(GroupByTransform.of("id", null).execute(this.ctx, input), hasSize(1));
    }

    @Test
    @DisplayName("UNION keeps integers that differ past double precision apart")
    void unionKeepsLargeIntegersApart() {
        JsonObject row = this.only(table("v", "UNION"), "[{\"id\":1,\"v\":[12345678901234567,12345678901234568]}]");
        assertThat(row.get("v").toString(), is(equalTo("[12345678901234567,12345678901234568]")));
    }

    @Test
    @DisplayName("MODE counts integers that differ past double precision apart")
    void modeCountsLargeIntegersApart() {
        JsonObject row = this.only(table("v", "MODE"), "[{\"id\":1,\"v\":12345678901234567},{\"id\":1,\"v\":12345678901234568},{\"id\":1,\"v\":12345678901234568}]");
        assertThat(row.get("v").toString(), is(equalTo("12345678901234568")));
    }

    @Test
    @DisplayName("Number keys past Gson's limits that round to one double stay apart")
    void keysPastGsonLimitsStayApart() {
        assertThat(this.group(null, "[{\"id\":1e10000},{\"id\":2e10000}]"), hasSize(2));
    }

    @Test
    @DisplayName("A number key past Gson's limits groups with an equal one inside them")
    void keyPastGsonLimitsGroupsByValue() {
        assertThat(this.group(null, "[{\"id\":1e10000},{\"id\":10e9999}]"), hasSize(1));
    }

    @Test
    @DisplayName("UNION keeps numbers past Gson's limits apart")
    void unionKeepsNumbersPastGsonLimitsApart() {
        JsonObject row = this.only(table("v", "UNION"), "[{\"id\":1,\"v\":[1e-10000,2e-10000]}]");
        assertThat(row.get("v").toString(), is(equalTo("[1e-10000,2e-10000]")));
    }

    @Test
    @DisplayName("MAX reads a number past Gson's limits")
    void maxReadsNumberPastGsonLimits() {
        JsonObject row = this.only(table("v", "MAX"), "[{\"id\":1,\"v\":5},{\"id\":1,\"v\":1e10000},{\"id\":1,\"v\":7}]");
        assertThat(row.get("v").toString(), is(equalTo("1e10000")));
    }

    @Test
    @DisplayName("MIN orders numbers past Gson's limits by exact value")
    void minOrdersNumbersPastGsonLimits() {
        JsonObject row = this.only(table("v", "MIN"), "[{\"id\":1,\"v\":1},{\"id\":1,\"v\":-1e-10000},{\"id\":1,\"v\":-2e-10000}]");
        assertThat(row.get("v").toString(), is(equalTo("-2e-10000")));
    }

    @Test
    @DisplayName("Fields keep their first-appearance order, then fields only the table names")
    void fieldOrder() {
        JsonObject row = this.only(table("n", "COUNT", "b", "LAST"), "[{\"id\":1,\"b\":1},{\"a\":2,\"id\":1,\"b\":3}]");
        assertThat(List.copyOf(row.keySet()), contains("id", "b", "a", "n"));
    }

    @Test
    @DisplayName("A folded value is a copy, so changing it leaves the input row unchanged")
    void foldedValueIsCopy() {
        List<JsonObject> input = rows("[{\"id\":1,\"v\":{\"k\":1}}]");
        JsonObject folded = GroupByTransform.of("id", null).execute(this.ctx, input).getFirst();
        folded.getAsJsonObject("v").addProperty("k", 2);
        assertThat(input.getFirst().toString(), is(equalTo("{\"id\":1,\"v\":{\"k\":1}}")));
    }

    @Test
    @DisplayName("A folded row is a new object even for a group of one")
    void foldedRowIsNew() {
        List<JsonObject> input = rows("[{\"id\":1}]");
        assertThat(GroupByTransform.of("id", null).execute(this.ctx, input).getFirst(), is(not(sameInstance(input.getFirst()))));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(this.group(null, null), is(nullValue()));
    }

    @Test
    @DisplayName("An empty list stays empty")
    void emptyStaysEmpty() {
        assertThat(this.group(null, "[]"), is(empty()));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<JsonObject> result = this.group(null, ROWS);
        assertThrows(UnsupportedOperationException.class, () -> result.add(new JsonObject()));
    }

    @Test
    @DisplayName("of rejects a name that is not an aggregate")
    void rejectsUnknownAggregate() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> GroupByTransform.of("id", table("v", "SUM")));
        assertThat(thrown.getMessage(), startsWith("Invalid GroupByTransform aggregate 'SUM'"));
    }

    @Test
    @DisplayName("of matches aggregate names exactly, so a lower-case name is rejected")
    void rejectsLowerCaseAggregate() {
        assertThrows(IllegalArgumentException.class, () -> GroupByTransform.of("id", table("v", "list")));
    }

    @Test
    @DisplayName("of rejects a table that aggregates the key field")
    void rejectsAggregatedKey() {
        assertThrows(IllegalArgumentException.class, () -> GroupByTransform.of("id", table("id", "COUNT")));
    }

    @Test
    @DisplayName("of rejects a table holding a null key")
    void rejectsNullAggregateKey() {
        Map<String, String> aggregates = new LinkedHashMap<>();
        aggregates.put(null, "LIST");
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> GroupByTransform.of("id", aggregates));
        assertThat(thrown.getMessage(), is(equalTo("GroupByTransform aggregates hold a null key")));
    }

    @Test
    @DisplayName("Fields only the table names appear in the table's order")
    void aggregatesKeepOrder() {
        JsonObject row = this.only(table("z", "LIST", "a", "COUNT", "m", "CONCAT"), "[{\"id\":1}]");
        assertThat(List.copyOf(row.keySet()), contains("id", "z", "a", "m"));
    }

    @Test
    @DisplayName("An absent aggregates table stays absent from the config")
    void absentAggregatesStayAbsent() {
        assertThat(GroupByTransform.of("id", null).config().has("aggregates"), is(false));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON, the table in order")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline(ROWS, table("v", "LIST", "n", "COUNT")));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The wire form carries the table as a JSON object in order")
    void wireFormCarriesTable() {
        String json = PipelineGson.toJson(pipeline(ROWS, table("v", "LIST", "n", "COUNT")));
        assertThat(json, containsString("\"aggregates\":{\"v\":\"LIST\",\"n\":\"COUNT\"}"));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> pipeline = pipeline(ROWS, table("v", "LIST", "n", "COUNT"));
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A pipeline folds each group through its table")
    void pipelineFoldsGroups() {
        Object result = pipeline(ROWS, table("v", "LIST", "n", "COUNT")).execute();
        assertThat(PipelineGson.gson().toJson(result), is(equalTo("[{\"id\":\"b\",\"v\":[1,3],\"n\":2},{\"id\":\"a\",\"v\":[2],\"n\":1}]")));
    }

    @Test
    @DisplayName("A wire table naming an unknown aggregate fails at load")
    void wireUnknownAggregateFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"RAW_JSON\",\"value\":\"[]\"},{\"kind\":\"PARSE_JSON\"},"
            + "{\"kind\":\"TRANSFORM_JSON_DESERIALIZE\",\"inputType\":\"JSON_ELEMENT\",\"outputType\":\"List<JSON_OBJECT>\"},"
            + "{\"kind\":\"TRANSFORM_GROUP_BY\",\"keyField\":\"id\",\"aggregates\":{\"v\":\"SUM\"}}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid GroupByTransform aggregate 'SUM'"));
    }

}
