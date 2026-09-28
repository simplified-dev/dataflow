package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ResolveAncestorTransform}: the walk to the root, what ends it, the copied value
 * and the depth, cycles, key comparison and duplicate keys, copying, and the wire.
 */
class ResolveAncestorTransformTest {

    /**
     * Turns single-quoted JSON into JSON, so a row literal needs no escaping.
     *
     * @param json the single-quoted JSON
     * @return the JSON
     */
    private static @NotNull String q(@NotNull String json) {
        return json.replace('\'', '"');
    }

    private static @NotNull List<JsonObject> rows(@NotNull String json) {
        List<JsonObject> rows = new ArrayList<>();
        for (JsonElement element : JsonParser.parseString(q(json)).getAsJsonArray()) rows.add(element.getAsJsonObject());
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static @NotNull String text(@Nullable Object rows) {
        if (rows == null) return "null";
        JsonArray array = new JsonArray();
        ((List<JsonObject>) rows).forEach(array::add);
        return array.toString();
    }

    private static @NotNull String resolve(@Nullable String valueField, @Nullable String depthField, @NotNull String json) {
        ResolveAncestorTransform stage = ResolveAncestorTransform.of("id", "parent", "root", valueField, depthField);
        return text(stage.execute(PipelineContext.defaults(), rows(json)));
    }

    @Test
    @DisplayName("A row with no parent is its own root at depth 0")
    void parentlessRowIsOwnRoot() {
        assertThat(resolve(null, "depth", "[{'id':'a'}]"), is(equalTo(q("[{'id':'a','root':'a','depth':0}]"))));
    }

    @Test
    @DisplayName("Every row of a parent chain resolves to the chain's root key")
    void chainResolvesToRootKey() {
        assertThat(resolve(null, null, "[{'id':'c','parent':'b'},{'id':'b','parent':'a'},{'id':'a'}]"),
            is(equalTo(q("[{'id':'c','parent':'b','root':'a'},{'id':'b','parent':'a','root':'a'},{'id':'a','root':'a'}]"))));
    }

    @Test
    @DisplayName("The depth field counts the parent steps to the root")
    void depthCountsSteps() {
        assertThat(resolve(null, "depth", "[{'id':'c','parent':'b'},{'id':'b','parent':'a'},{'id':'a'}]"),
            is(equalTo(q("[{'id':'c','parent':'b','root':'a','depth':2},{'id':'b','parent':'a','root':'a','depth':1},{'id':'a','root':'a','depth':0}]"))));
    }

    @Test
    @DisplayName("A value field copies the root's field instead of its key")
    void valueFieldCopiesRootField() {
        assertThat(resolve("name", null, "[{'id':'b','parent':'a'},{'id':'a','name':'Alpha'}]"),
            is(equalTo(q("[{'id':'b','parent':'a','root':'Alpha'},{'id':'a','name':'Alpha','root':'Alpha'}]"))));
    }

    @Test
    @DisplayName("A root without the value field leaves the output off and still writes the depth")
    void missingRootValueLeavesOutputOff() {
        assertThat(resolve("name", "depth", "[{'id':'b','parent':'a'},{'id':'a'}]"),
            is(equalTo(q("[{'id':'b','parent':'a','depth':1},{'id':'a','depth':0}]"))));
    }

    @Test
    @DisplayName("A root whose value field is JSON null leaves the output off")
    void nullRootValueLeavesOutputOff() {
        assertThat(resolve("name", null, "[{'id':'a','name':null}]"), is(equalTo(q("[{'id':'a','name':null}]"))));
    }

    @Test
    @DisplayName("A row carrying no key and no parent takes no output but depth 0")
    void keylessRowTakesDepthOnly() {
        assertThat(resolve(null, "depth", "[{'name':'x'}]"), is(equalTo(q("[{'name':'x','depth':0}]"))));
    }

    @Test
    @DisplayName("A parent naming a key the list lacks makes that row the root")
    void unknownParentMakesRowRoot() {
        assertThat(resolve(null, "depth", "[{'id':'c','parent':'b'},{'id':'b','parent':'x'}]"),
            is(equalTo(q("[{'id':'c','parent':'b','root':'b','depth':1},{'id':'b','parent':'x','root':'b','depth':0}]"))));
    }

    @Test
    @DisplayName("A null, empty array or empty object parent is no parent")
    void emptyParentsAreNoParent() {
        assertThat(resolve(null, null, "[{'id':'a','parent':null},{'id':'b','parent':[]},{'id':'c','parent':{}}]"),
            is(equalTo(q("[{'id':'a','parent':null,'root':'a'},{'id':'b','parent':[],'root':'b'},{'id':'c','parent':{},'root':'c'}]"))));
    }

    @Test
    @DisplayName("An empty-string parent is no parent, even when a row is keyed by the empty string")
    void emptyStringParentIsNoParent() {
        assertThat(resolve(null, null, "[{'id':''},{'id':'b','parent':''}]"),
            is(equalTo(q("[{'id':'','root':''},{'id':'b','parent':'','root':'b'}]"))));
    }

    @Test
    @DisplayName("A cycle leaves the output and depth fields off")
    void cycleLeavesFieldsOff() {
        assertThat(resolve(null, "depth", "[{'id':'a','parent':'b'},{'id':'b','parent':'a'}]"),
            is(equalTo(q("[{'id':'a','parent':'b'},{'id':'b','parent':'a'}]"))));
    }

    @Test
    @DisplayName("A cycle removes an output and depth field the row already held")
    void cycleRemovesHeldFields() {
        assertThat(resolve(null, "depth", "[{'id':'a','parent':'b','root':'old','depth':9},{'id':'b','parent':'a'}]"),
            is(equalTo(q("[{'id':'a','parent':'b'},{'id':'b','parent':'a'}]"))));
    }

    @Test
    @DisplayName("A cycle leaves no direct parent behind when the parent field is also the output field")
    void cycleOverParentFieldLeavesNoGuess() {
        List<JsonObject> resolved = ResolveAncestorTransform.of("id", "region", "region", null, null)
            .execute(PipelineContext.defaults(), rows("[{'id':'a','region':'b'},{'id':'b','region':'a'}]"));
        assertThat(text(resolved), is(equalTo(q("[{'id':'a'},{'id':'b'}]"))));
    }

    @Test
    @DisplayName("A root without the value field removes an output field the row already held")
    void missingRootValueRemovesHeldOutput() {
        assertThat(resolve("name", null, "[{'id':'a','root':'old'}]"), is(equalTo(q("[{'id':'a'}]"))));
    }

    @Test
    @DisplayName("A row naming itself as parent is a cycle")
    void selfParentIsCycle() {
        assertThat(resolve(null, null, "[{'id':'a','parent':'a'}]"), is(equalTo(q("[{'id':'a','parent':'a'}]"))));
    }

    @Test
    @DisplayName("A row whose chain runs into a cycle is left unresolved")
    void chainIntoCycleUnresolved() {
        assertThat(resolve(null, null, "[{'id':'c','parent':'a'},{'id':'a','parent':'b'},{'id':'b','parent':'a'}]"),
            is(equalTo(q("[{'id':'c','parent':'a'},{'id':'a','parent':'b'},{'id':'b','parent':'a'}]"))));
    }

    @Test
    @DisplayName("Keys compare by string form, so a numeric parent reaches a text key")
    void numericParentReachesTextKey() {
        assertThat(resolve(null, null, "[{'id':'1'},{'id':'2','parent':1}]"),
            is(equalTo(q("[{'id':'1','root':'1'},{'id':'2','parent':1,'root':'1'}]"))));
    }

    @Test
    @DisplayName("When two rows carry one key, a parent reference reaches the first")
    void parentReachesFirstRow() {
        List<JsonObject> resolved = ResolveAncestorTransform.of("id", "parent", "root", "name", null)
            .execute(PipelineContext.defaults(), rows("[{'id':'a','name':'first'},{'id':'a','name':'second'},{'id':'b','parent':'a'}]"));
        assertThat(resolved.getLast().get("root").getAsString(), is(equalTo("first")));
    }

    @Test
    @DisplayName("An output field the row already holds is overwritten in place")
    void existingOutputOverwritten() {
        assertThat(resolve(null, null, "[{'id':'a','root':'old','x':1}]"), is(equalTo(q("[{'id':'a','root':'a','x':1}]"))));
    }

    @Test
    @DisplayName("The input rows are not mutated")
    void inputNotMutated() {
        List<JsonObject> input = rows("[{'id':'b','parent':'a'},{'id':'a'}]");
        ResolveAncestorTransform.of("id", "parent", "root", null, "depth").execute(PipelineContext.defaults(), input);
        assertThat(text(input), is(equalTo(q("[{'id':'b','parent':'a'},{'id':'a'}]"))));
    }

    @Test
    @DisplayName("The copied root value is not shared with the root row")
    void rootValueIsCopy() {
        List<JsonObject> resolved = ResolveAncestorTransform.of("id", "parent", "root", "meta", null)
            .execute(PipelineContext.defaults(), rows("[{'id':'a','meta':{'x':1}},{'id':'b','parent':'a'}]"));
        resolved.getLast().getAsJsonObject("root").addProperty("y", 2);
        assertThat(resolved.getFirst().get("meta").toString(), is(equalTo(q("{'x':1}"))));
    }

    @Test
    @DisplayName("A null input rejects with null")
    void nullInputRejects() {
        assertThat(ResolveAncestorTransform.of("id", "parent", "root", null, null).execute(PipelineContext.defaults(), null), is(nullValue()));
    }

    @Test
    @DisplayName("An empty input yields an empty list")
    void emptyInputYieldsEmpty() {
        assertThat(resolve(null, null, "[]"), is(equalTo("[]")));
    }

    @Test
    @DisplayName("The output list is unmodifiable")
    void outputUnmodifiable() {
        List<JsonObject> resolved = ResolveAncestorTransform.of("id", "parent", "root", null, null)
            .execute(PipelineContext.defaults(), rows("[{'id':'a'}]"));
        assertThrows(UnsupportedOperationException.class, () -> resolved.add(new JsonObject()));
    }

    private static @NotNull DataPipeline<?> wirePipeline(@Nullable String valueField, @Nullable String depthField) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(q("[{'id':'c','parent':'b'},{'id':'b','parent':'a'},{'id':'a','name':'Alpha'}]")))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .stage(ResolveAncestorTransform.of("id", "parent", "root", valueField, depthField))
            .build();
    }

    @Test
    @DisplayName("A resolve round-trips on the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(wirePipeline("name", "depth"));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A resolve round-trips on the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> original = wirePipeline("name", "depth");
        assertThat(text(PipelineGson.fromJson(PipelineGson.toJson(original)).execute()), is(equalTo(text(original.execute()))));
    }

    @Test
    @DisplayName("A resolve without its optional fields writes neither key")
    void wireOmitsAbsentOptionals() {
        assertThat(PipelineGson.toJson(wirePipeline(null, null)), not(anyOf(containsString("\"valueField\""), containsString("\"depthField\""))));
    }

    @Test
    @DisplayName("A resolve without its optional fields round-trips to the same output")
    void wireRoundTripWithoutOptionals() {
        DataPipeline<?> original = wirePipeline(null, null);
        assertThat(text(PipelineGson.fromJson(PipelineGson.toJson(original)).execute()), is(equalTo(text(original.execute()))));
    }

}
