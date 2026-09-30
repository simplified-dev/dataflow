package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataPipelineResolver;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link JoinByKeyTransform}: the fill rule, the three modes, key comparison, duplicate
 * and missing keys, copying, the operand's single evaluation, factory rejections and the wire.
 */
class JoinByKeyTransformTest {

    private static final class MapResolver implements DataPipelineResolver {

        private final @NotNull Map<String, DataPipeline<?>> pipelines = new HashMap<>();

        @Override
        public @NotNull Optional<DataPipeline<?>> resolve(@NotNull String id) {
            return Optional.ofNullable(this.pipelines.get(id));
        }

        @Override
        public @Nullable String idOf(@NotNull DataPipeline<?> pipeline) {
            return null;
        }

    }

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

    private static @NotNull DataPipeline<List<JsonObject>> operand(@NotNull String json) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(q(json)))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .build();
    }

    private static @NotNull String text(@Nullable List<JsonObject> rows) {
        if (rows == null) return "null";
        JsonArray array = new JsonArray();
        rows.forEach(array::add);
        return array.toString();
    }

    @SuppressWarnings("unchecked")
    private static @NotNull String textOf(@Nullable Object rows) {
        return text((List<JsonObject>) rows);
    }

    private static @NotNull String join(@NotNull String mode, @Nullable String columns, @NotNull String left, @NotNull String right) {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "id", mode, columns, operand(right));
        return text(stage.execute(PipelineContext.defaults(), rows(left)));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    @Test
    @DisplayName("A matched row takes a right value into a cell the left row lacks")
    void matchedRowTakesMissingCell() {
        assertThat(join("LEFT", null, "[{'id':'a','x':1}]", "[{'id':'a','name':'A'}]"),
            is(equalTo(q("[{'id':'a','x':1,'name':'A'}]"))));
    }

    @Test
    @DisplayName("The left value stands over a populated right value")
    void leftValueStands() {
        assertThat(join("LEFT", null, "[{'id':'a','name':'L'}]", "[{'id':'a','name':'R'}]"),
            is(equalTo(q("[{'id':'a','name':'L'}]"))));
    }

    @Test
    @DisplayName("A null, empty string, empty array or empty object left cell is filled in place")
    void emptyLeftCellsFilled() {
        assertThat(join("LEFT", null, "[{'id':'a','p':null,'q':'','r':[],'s':{}}]", "[{'id':'a','p':1,'q':2,'r':3,'s':4}]"),
            is(equalTo(q("[{'id':'a','p':1,'q':2,'r':3,'s':4}]"))));
    }

    @Test
    @DisplayName("A right value that is itself empty never fills a cell")
    void emptyRightValueDoesNotFill() {
        assertThat(join("LEFT", null, "[{'id':'a'}]", "[{'id':'a','p':'','q':null,'r':[],'s':{}}]"),
            is(equalTo(q("[{'id':'a'}]"))));
    }

    @Test
    @DisplayName("An empty left cell no right value populates keeps its empty value")
    void unfilledEmptyLeftCellKept() {
        assertThat(join("LEFT", null, "[{'id':'a','n':''}]", "[{'id':'a'}]"), is(equalTo(q("[{'id':'a','n':''}]"))));
    }

    @Test
    @DisplayName("A zero or false right value fills a cell")
    void zeroAndFalseFill() {
        assertThat(join("LEFT", null, "[{'id':'a'}]", "[{'id':'a','n':0,'b':false}]"),
            is(equalTo(q("[{'id':'a','n':0,'b':false}]"))));
    }

    @Test
    @DisplayName("columns limits the right keys that may fill a cell")
    void columnsLimitFill() {
        assertThat(join("LEFT", "name", "[{'id':'a'}]", "[{'id':'a','name':'A','lore':'L'}]"),
            is(equalTo(q("[{'id':'a','name':'A'}]"))));
    }

    @Test
    @DisplayName("columns entries are trimmed and blank entries skipped")
    void columnsTrimmed() {
        assertThat(join("LEFT", " name , ,lore", "[{'id':'a'}]", "[{'id':'a','name':'A','lore':'L','x':1}]"),
            is(equalTo(q("[{'id':'a','name':'A','lore':'L'}]"))));
    }

    @Test
    @DisplayName("INNER keeps only the matched left rows")
    void innerKeepsMatched() {
        assertThat(join("INNER", null, "[{'id':'a'},{'id':'b'}]", "[{'id':'b','v':1},{'id':'c','v':2}]"),
            is(equalTo(q("[{'id':'b','v':1}]"))));
    }

    @Test
    @DisplayName("LEFT keeps every left row and leaves an unmatched one unchanged")
    void leftKeepsEveryRow() {
        assertThat(join("LEFT", null, "[{'id':'a'},{'id':'b'}]", "[{'id':'b','v':1},{'id':'c','v':2}]"),
            is(equalTo(q("[{'id':'a'},{'id':'b','v':1}]"))));
    }

    @Test
    @DisplayName("FULL appends the right-only rows after the left rows, in right order")
    void fullAppendsRightOnlyInRightOrder() {
        assertThat(join("FULL", null, "[{'id':'b'}]", "[{'id':'c','v':1},{'id':'b','v':2},{'id':'a','v':3}]"),
            is(equalTo(q("[{'id':'b','v':2},{'id':'c','v':1},{'id':'a','v':3}]"))));
    }

    @Test
    @DisplayName("A right-only row carries its key under the left key")
    void rightOnlyRowKeyedByLeftKey() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "key", "FULL", null, operand("[{'key':'a','v':1}]"));
        assertThat(text(stage.execute(PipelineContext.defaults(), rows("[]"))), is(equalTo(q("[{'id':'a','v':1}]"))));
    }

    @Test
    @DisplayName("An empty left key is never filled from a right cell named like it")
    void emptyLeftKeyNotFilled() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "key", "LEFT", null, operand("[{'key':'','id':'X','v':1}]"));
        assertThat(text(stage.execute(PipelineContext.defaults(), rows("[{'id':''}]"))), is(equalTo(q("[{'id':'','v':1}]"))));
    }

    @Test
    @DisplayName("A right-only row keeps its empty key over a right cell named like the left key")
    void rightOnlyRowKeepsEmptyKey() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "key", "FULL", null, operand("[{'key':'','id':'X'}]"));
        assertThat(text(stage.execute(PipelineContext.defaults(), rows("[]"))), is(equalTo(q("[{'id':''}]"))));
    }

    @Test
    @DisplayName("A right-only row takes only the allowed columns")
    void rightOnlyRowTakesAllowedColumns() {
        assertThat(join("FULL", "v", "[]", "[{'id':'a','v':1,'w':2}]"), is(equalTo(q("[{'id':'a','v':1}]"))));
    }

    @Test
    @DisplayName("A right-only row keeps its key's JSON type")
    void rightOnlyRowKeepsKeyType() {
        assertThat(join("FULL", null, "[]", "[{'id':7}]"), is(equalTo(q("[{'id':7}]"))));
    }

    @Test
    @DisplayName("Keys compare by string form, so a number matches its text")
    void numberMatchesText() {
        assertThat(join("INNER", null, "[{'id':1}]", "[{'id':'1','v':'x'}]"), is(equalTo(q("[{'id':1,'v':'x'}]"))));
    }

    @Test
    @DisplayName("An object key compares by its compact JSON")
    void objectKeyMatchesByJson() {
        assertThat(join("INNER", null, "[{'id':{'a':1}}]", "[{'id':{'a':1},'v':2}]"), is(equalTo(q("[{'id':{'a':1},'v':2}]"))));
    }

    @Test
    @DisplayName("When two right rows carry one key, the first is the one matched")
    void firstRightRowWins() {
        assertThat(join("LEFT", null, "[{'id':'a'}]", "[{'id':'a','v':1},{'id':'a','v':2,'w':3}]"),
            is(equalTo(q("[{'id':'a','v':1}]"))));
    }

    @Test
    @DisplayName("FULL appends a duplicated right key once, from its first row")
    void fullAppendsDuplicateOnce() {
        assertThat(join("FULL", null, "[]", "[{'id':'a','v':1},{'id':'a','v':2}]"), is(equalTo(q("[{'id':'a','v':1}]"))));
    }

    @Test
    @DisplayName("FULL appends no later right row repeating a key a left row matched")
    void fullSkipsDuplicateOfMatchedKey() {
        assertThat(join("FULL", null, "[{'id':'a'}]", "[{'id':'a','v':1},{'id':'a','v':2}]"), is(equalTo(q("[{'id':'a','v':1}]"))));
    }

    @Test
    @DisplayName("Left rows sharing a key each take the matching right row")
    void duplicateLeftRowsEachJoin() {
        assertThat(join("LEFT", null, "[{'id':'a'},{'id':'a','n':1}]", "[{'id':'a','v':1}]"),
            is(equalTo(q("[{'id':'a','v':1},{'id':'a','n':1,'v':1}]"))));
    }

    @Test
    @DisplayName("LEFT keeps a left row that carries no key")
    void leftKeepsKeylessRow() {
        assertThat(join("LEFT", null, "[{'x':1},{'id':null}]", "[{'id':'a','v':1}]"), is(equalTo(q("[{'x':1},{'id':null}]"))));
    }

    @Test
    @DisplayName("INNER drops a left row that carries no key")
    void innerDropsKeylessRow() {
        assertThat(join("INNER", null, "[{'x':1}]", "[{'id':'a','v':1}]"), is(equalTo("[]")));
    }

    @Test
    @DisplayName("A right row that carries no key is never matched or appended")
    void keylessRightRowIgnored() {
        assertThat(join("FULL", null, "[]", "[{'v':1},{'id':null,'v':2}]"), is(equalTo("[]")));
    }

    @Test
    @DisplayName("The right key is not copied into a matched row when it differs from the left key")
    void rightKeyNotCopied() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("name", "displayName", "LEFT", null, operand("[{'displayName':'A','id':'x'}]"));
        assertThat(text(stage.execute(PipelineContext.defaults(), rows("[{'name':'A'}]"))), is(equalTo(q("[{'name':'A','id':'x'}]"))));
    }

    @Test
    @DisplayName("An empty operand leaves LEFT rows unchanged")
    void emptyOperandJoinsNothing() {
        assertThat(join("LEFT", null, "[{'id':'a'}]", "[]"), is(equalTo(q("[{'id':'a'}]"))));
    }

    @Test
    @DisplayName("Chained FULL joins fill each id's cells from the first input that populates them")
    void chainedJoinsMergeInInputOrder() {
        DataPipeline<List<JsonObject>> pipeline = DataPipeline.builder()
            .source(LiteralSource.rawJson(q("[{'id':'a','n':'A'},{'id':'b'}]")))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .stage(JoinByKeyTransform.of("id", "id", "FULL", null, operand("[{'id':'b','n':'B','x':1},{'id':'c','n':'C'}]")))
            .stage(JoinByKeyTransform.of("id", "id", "FULL", null, operand("[{'id':'a','n':'Z','x':2},{'id':'d'}]")))
            .build();
        assertThat(text(pipeline.execute()),
            is(equalTo(q("[{'id':'a','n':'A','x':2},{'id':'b','n':'B','x':1},{'id':'c','n':'C'},{'id':'d'}]"))));
    }

    @Test
    @DisplayName("A null input rejects with null")
    void nullInputRejects() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "id", "LEFT", null, operand("[]"));
        assertThat(stage.execute(PipelineContext.defaults(), null), is(nullValue()));
    }

    @Test
    @DisplayName("A null operand output rejects with null")
    void nullOperandRejects() {
        assertThat(join("LEFT", null, "[{'id':'a'}]", "{'id':'a'}"), is(equalTo("null")));
    }

    @Test
    @DisplayName("The input rows are not mutated")
    void inputRowsNotMutated() {
        List<JsonObject> left = rows("[{'id':'a'}]");
        JoinByKeyTransform.of("id", "id", "LEFT", null, operand("[{'id':'a','v':1}]")).execute(PipelineContext.defaults(), left);
        assertThat(text(left), is(equalTo(q("[{'id':'a'}]"))));
    }

    @Test
    @DisplayName("The operand's rows are not mutated")
    void operandRowsNotMutated() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "id", "FULL", null, operand("[{'id':'a','v':1},{'id':'b','w':{'x':1}}]"));
        PipelineContext ctx = PipelineContext.defaults();
        List<JsonObject> joined = stage.execute(ctx, rows("[{'id':'a','n':1}]"));
        joined.getLast().getAsJsonObject("w").addProperty("y", 2);
        assertThat(text(ctx.evaluateOperand(stage.right())), is(equalTo(q("[{'id':'a','v':1},{'id':'b','w':{'x':1}}]"))));
    }

    @Test
    @DisplayName("An unmatched row is a copy, not the input instance")
    void unmatchedRowIsCopy() {
        List<JsonObject> left = rows("[{'id':'a'}]");
        List<JsonObject> joined = JoinByKeyTransform.of("id", "id", "LEFT", null, operand("[]")).execute(PipelineContext.defaults(), left);
        assertThat(joined.getFirst(), is(not(sameInstance(left.getFirst()))));
    }

    @Test
    @DisplayName("The output list is unmodifiable")
    void outputUnmodifiable() {
        List<JsonObject> joined = JoinByKeyTransform.of("id", "id", "LEFT", null, operand("[]"))
            .execute(PipelineContext.defaults(), rows("[{'id':'a'}]"));
        assertThrows(UnsupportedOperationException.class, () -> joined.add(new JsonObject()));
    }

    @Test
    @DisplayName("The operand is read once per context however many times the stage runs")
    void operandReadOncePerContext() {
        LiteralSource<String> source = LiteralSource.rawJson(q("[{'id':'a','v':1}]"));
        DataPipeline<List<JsonObject>> right = DataPipeline.builder()
            .source(source)
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .build();
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "id", "LEFT", null, right);
        AtomicInteger runs = new AtomicInteger();
        PipelineContext ctx = PipelineContext.builder()
            .withTrace((traced, output) -> {
                if (traced == source) runs.incrementAndGet();
            })
            .build();

        stage.execute(ctx, rows("[{'id':'a'}]"));
        stage.execute(ctx, rows("[{'id':'b'}]"));

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("A tracer sees only registered stages, and no step for the indexing")
    void tracerSeesOnlyRegisteredStages() {
        JoinByKeyTransform stage = JoinByKeyTransform.of("id", "id", "LEFT", null, operand("[{'id':'a','v':1}]"));
        List<String> unregistered = new ArrayList<>();
        PipelineContext ctx = PipelineContext.builder()
            .withTrace((traced, output) -> {
                if (!traced.getClass().isAnnotationPresent(StageSpec.class)) unregistered.add(traced.getClass().getName());
            })
            .build();
        stage.execute(ctx, rows("[{'id':'a'}]"));
        assertThat(unregistered, is(empty()));
    }

    @Test
    @DisplayName("of rejects a mode that names no join mode")
    void rejectsUnknownMode() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> JoinByKeyTransform.of("id", "id", "OUTER", null, operand("[]")));
        assertThat(thrown.getMessage(), startsWith("Unknown JoinByKeyTransform mode 'OUTER'"));
    }

    @Test
    @DisplayName("of rejects a mode in the wrong case")
    void rejectsLowerCaseMode() {
        assertThrows(IllegalArgumentException.class, () -> JoinByKeyTransform.of("id", "id", "left", null, operand("[]")));
    }

    @Test
    @DisplayName("of rejects a column list that names no key")
    void rejectsEmptyColumns() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> JoinByKeyTransform.of("id", "id", "LEFT", " , ", operand("[]")));
        assertThat(thrown.getMessage(), containsString("names no key"));
    }

    @Test
    @DisplayName("of rejects an operand that does not produce List<JSON_OBJECT>")
    void rejectsWrongOperandType() {
        DataPipeline<String> wrong = DataPipeline.builder().source(LiteralSource.text("x")).build();
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> JoinByKeyTransform.of("id", "id", "LEFT", null, wrong));
        assertThat(thrown.getMessage(), startsWith("Invalid JoinByKeyTransform operand"));
    }

    private static @NotNull DataPipeline<?> wirePipeline(@Nullable String columns) {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(q("[{'id':'a'},{'id':'b','name':'L'}]")))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.JSON_OBJECT)))
            .stage(JoinByKeyTransform.of("id", "id", "FULL", columns, operand("[{'id':'b','name':'R','x':1},{'id':'c','name':'C'}]")))
            .build();
    }

    @Test
    @DisplayName("A join round-trips on the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(wirePipeline("name"));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A join round-trips on the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> original = wirePipeline("name");
        Object rebuilt = PipelineGson.fromJson(PipelineGson.toJson(original)).execute();
        assertThat(textOf(rebuilt), is(equalTo(textOf(original.execute()))));
    }

    @Test
    @DisplayName("A join without columns writes no columns key")
    void wireOmitsAbsentColumns() {
        assertThat(PipelineGson.toJson(wirePipeline(null)), not(containsString("\"columns\"")));
    }

    @Test
    @DisplayName("A join without columns round-trips to the same JSON")
    void wireRoundTripWithoutColumns() {
        String first = PipelineGson.toJson(wirePipeline(null));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    /**
     * Wire stages reading the literal rows {@code [{"id":"a"}]} as {@code List<JSON_OBJECT>}.
     */
    private static final @NotNull String WIRE_ROWS = "{'kind':'SOURCE_LITERAL','outputType':'RAW_JSON','value':'[{\\'id\\':\\'a\\'}]'},"
        + "{'kind':'PARSE_JSON'},"
        + "{'kind':'TRANSFORM_JSON_DESERIALIZE','inputType':'JSON_ELEMENT','outputType':'List<JSON_OBJECT>'}";

    @Test
    @DisplayName("A SOURCE_EMBED operand on the wire joins the saved pipeline's rows")
    void embedOperandFromWire() {
        String json = q("[" + WIRE_ROWS + ","
            + "{'kind':'TRANSFORM_JOIN_BY_KEY','leftKey':'id','rightKey':'id','mode':'LEFT',"
            + "'right':[{'kind':'SOURCE_EMBED','embeddedPipelineId':'saved','outputType':'List<JSON_OBJECT>'}]}]");
        MapResolver resolver = new MapResolver();
        resolver.pipelines.put("saved", operand("[{'id':'a','name':'A'}]"));
        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();
        assertThat(textOf(PipelineGson.fromJson(json).execute(ctx)), is(equalTo(q("[{'id':'a','name':'A'}]"))));
    }

    @Test
    @DisplayName("An operand of the wrong output type fails the load with the factory's message")
    void wrongOperandTypeRejectedAtLoad() {
        String json = q("[" + WIRE_ROWS + ","
            + "{'kind':'TRANSFORM_JOIN_BY_KEY','leftKey':'id','rightKey':'id','mode':'LEFT',"
            + "'right':[{'kind':'SOURCE_LITERAL','outputType':'STRING','value':'x'}]}]");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid JoinByKeyTransform operand"));
    }

    @Test
    @DisplayName("An unknown mode fails the load with the factory's message")
    void unknownModeRejectedAtLoad() {
        String json = q("[" + WIRE_ROWS + ","
            + "{'kind':'TRANSFORM_JOIN_BY_KEY','leftKey':'id','rightKey':'id','mode':'OUTER',"
            + "'right':[" + WIRE_ROWS + "]}]");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Unknown JoinByKeyTransform mode 'OUTER'"));
    }

}
