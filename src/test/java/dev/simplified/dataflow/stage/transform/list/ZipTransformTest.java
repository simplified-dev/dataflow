package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.dom.CssSelectTransform;
import dev.simplified.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.FieldTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZipTransformTest {

    private static final @NotNull String PET = "{\"ids\":[\"A\",\"B\",\"C\"],\"rarities\":[\"COMMON\",\"RARE\"]}";

    private static final @NotNull DataType<List<String>> STRINGS = DataType.list(DataTypes.STRING);

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

    private static @NotNull List<Stage<?, ?>> field(@NotNull String name, @NotNull DataType<?> element) {
        return List.of(FieldTransform.of(name), DeserializeTransform.of(DataType.list(element)));
    }

    private static @NotNull ZipTransform<JsonObject, String, String> pets(@Nullable String mode) {
        return ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            field("ids", DataTypes.STRING), field("rarities", DataTypes.STRING),
            "id", "rarity", mode
        );
    }

    private static @NotNull ZipTransform<List<String>, String, String> mirrored() {
        return ZipTransform.of(
            STRINGS, DataTypes.STRING, DataTypes.STRING,
            List.of(ReverseTransform.of(DataTypes.STRING), ReverseTransform.of(DataTypes.STRING)),
            List.of(ReverseTransform.of(DataTypes.STRING)),
            "l", "r", null
        );
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(PET))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataTypes.JSON_OBJECT))
            .stage(pets("LONGEST"))
            .build();
    }

    @Test
    @DisplayName("SHORTEST pairs the k-th elements and stops at the shorter list")
    void shortestStopsAtShorter() {
        List<JsonObject> result = pets("SHORTEST").execute(this.ctx, object(PET));
        assertThat(json(result), is(equalTo("[{\"id\":\"A\",\"rarity\":\"COMMON\"},{\"id\":\"B\",\"rarity\":\"RARE\"}]")));
    }

    @Test
    @DisplayName("An absent mode stops at the shorter list")
    void absentModeIsShortest() {
        assertThat(pets(null).execute(this.ctx, object(PET)), hasSize(2));
    }

    @Test
    @DisplayName("LONGEST runs to the longer list, omitting the right key once it runs out")
    void longestOmitsRightKey() {
        List<JsonObject> result = pets("LONGEST").execute(this.ctx, object(PET));
        assertThat(json(result.getLast()), is(equalTo("{\"id\":\"C\"}")));
    }

    @Test
    @DisplayName("LONGEST omits the left key once the left list runs out")
    void longestOmitsLeftKey() {
        List<JsonObject> result = pets("LONGEST").execute(this.ctx, object("{\"ids\":[\"A\"],\"rarities\":[\"COMMON\",\"RARE\"]}"));
        assertThat(json(result), is(equalTo("[{\"id\":\"A\",\"rarity\":\"COMMON\"},{\"rarity\":\"RARE\"}]")));
    }

    @Test
    @DisplayName("A null element omits its key")
    void nullElementOmitsKey() {
        List<JsonObject> result = mirrored().execute(this.ctx, Arrays.asList("a", null, "c", "d"));
        assertThat(json(result), is(equalTo("[{\"l\":\"a\",\"r\":\"d\"},{\"r\":\"c\"},{\"l\":\"c\"},{\"l\":\"d\",\"r\":\"a\"}]")));
    }

    @Test
    @DisplayName("A position where both elements are null keeps an empty object, so later pairs keep their positions")
    void bothNullKeepsPosition() {
        List<JsonObject> result = mirrored().execute(this.ctx, Arrays.asList("a", null, "b"));
        assertThat(json(result), is(equalTo("[{\"l\":\"a\",\"r\":\"b\"},{},{\"l\":\"b\",\"r\":\"a\"}]")));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(pets(null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A null from the left body rejects with null")
    void nullLeftRejects() {
        assertThat(pets(null).execute(this.ctx, object("{\"rarities\":[\"COMMON\"]}")), is(nullValue()));
    }

    @Test
    @DisplayName("A null from the right body rejects with null")
    void nullRightRejects() {
        assertThat(pets(null).execute(this.ctx, object("{\"ids\":[\"A\"]}")), is(nullValue()));
    }

    @Test
    @DisplayName("Two empty lists give an empty list")
    void emptyListsGiveEmpty() {
        assertThat(pets("LONGEST").execute(this.ctx, object("{\"ids\":[],\"rarities\":[]}")), is(empty()));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<JsonObject> result = pets(null).execute(this.ctx, object(PET));
        assertThrows(UnsupportedOperationException.class, () -> result.add(new JsonObject()));
    }

    @Test
    @DisplayName("A numeric element is written as a JSON number")
    void numericElementWrittenAsNumber() {
        ZipTransform<JsonObject, String, Integer> stage = ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.INT,
            field("ids", DataTypes.STRING), field("costs", DataTypes.INT), "id", "cost", null
        );
        assertThat(json(stage.execute(this.ctx, object("{\"ids\":[\"A\"],\"costs\":[5]}"))), is(equalTo("[{\"id\":\"A\",\"cost\":5}]")));
    }

    @Test
    @DisplayName("A JSON element is copied, so changing the output leaves the input unchanged")
    void jsonElementCopied() {
        JsonObject input = object("{\"rows\":[{\"k\":1}],\"ids\":[\"A\"]}");
        ZipTransform<JsonObject, JsonObject, String> stage = ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.JSON_OBJECT, DataTypes.STRING,
            field("rows", DataTypes.JSON_OBJECT), field("ids", DataTypes.STRING), "row", "id", null
        );
        stage.execute(this.ctx, input).getFirst().getAsJsonObject("row").addProperty("k", 2);
        assertThat(json(input), is(equalTo("{\"rows\":[{\"k\":1}],\"ids\":[\"A\"]}")));
    }

    @Test
    @DisplayName("of rejects a left body that does not yield the left list type")
    void rejectsMistypedLeft() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.INT, DataTypes.STRING,
            field("ids", DataTypes.STRING), field("rarities", DataTypes.STRING), "id", "rarity", null
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ZipTransform left body"));
    }

    @Test
    @DisplayName("of rejects a right body that does not consume the input type")
    void rejectsMistypedRight() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            field("ids", DataTypes.STRING), List.of(UpperCaseTransform.of()), "id", "rarity", null
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ZipTransform right body"));
    }

    @Test
    @DisplayName("of rejects an empty body")
    void rejectsEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> ZipTransform.of(
            STRINGS, DataTypes.STRING, DataTypes.STRING, List.of(), List.of(ReverseTransform.of(DataTypes.STRING)), "l", "r", null
        ));
    }

    @Test
    @DisplayName("of rejects a DOM_NODE element type, which Gson cannot write")
    void rejectsDomNode() {
        List<Stage<?, ?>> nodes = List.of(ParseHtmlTransform.of(), CssSelectTransform.of("p"));
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ZipTransform.of(
            DataTypes.RAW_HTML, DataTypes.DOM_NODE, DataTypes.DOM_NODE, nodes, nodes, "l", "r", null
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ZipTransform leftType"));
    }

    @Test
    @DisplayName("of rejects the same key for both sides")
    void rejectsSameKeys() {
        assertThrows(IllegalArgumentException.class, () -> ZipTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, DataTypes.STRING,
            field("ids", DataTypes.STRING), field("rarities", DataTypes.STRING), "id", "id", null
        ));
    }

    @Test
    @DisplayName("of rejects a mode that is not a Mode name")
    void rejectsUnknownMode() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> pets("ALL"));
        assertThat(thrown.getMessage(), startsWith("Invalid ZipTransform mode 'ALL'"));
    }

    @Test
    @DisplayName("of matches mode names exactly, so a lower-case mode is rejected")
    void rejectsLowerCaseMode() {
        assertThrows(IllegalArgumentException.class, () -> pets("longest"));
    }

    @Test
    @DisplayName("An absent mode stays absent from the config")
    void absentModeStaysAbsent() {
        assertThat(pets(null).config().has("mode"), is(false));
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
    @DisplayName("A wire left body that does not yield the left list type fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a,b\"},"
            + "{\"kind\":\"TRANSFORM_ZIP\",\"inputType\":\"STRING\",\"leftType\":\"INT\",\"rightType\":\"STRING\","
            + "\"left\":[{\"kind\":\"TRANSFORM_SPLIT\",\"regex\":\",\"}],\"right\":[{\"kind\":\"TRANSFORM_SPLIT\",\"regex\":\",\"}],"
            + "\"leftKey\":\"l\",\"rightKey\":\"r\"}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid ZipTransform left body"));
    }

}
