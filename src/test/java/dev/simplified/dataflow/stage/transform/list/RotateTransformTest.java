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
import dev.simplified.dataflow.stage.terminal.collect.FirstCollect;
import dev.simplified.dataflow.stage.transform.json.AsIntTransform;
import dev.simplified.dataflow.stage.transform.json.AsStringTransform;
import dev.simplified.dataflow.stage.transform.json.DeserializeTransform;
import dev.simplified.dataflow.stage.transform.json.FieldTransform;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RotateTransformTest {

    private static final @NotNull String ZOO = "{\"pets\":[\"A\",\"B\",\"C\",\"D\",\"E\",\"F\"],\"offset\":2}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull JsonObject zoo(int offset) {
        JsonObject zoo = JsonParser.parseString(ZOO).getAsJsonObject();
        zoo.addProperty("offset", offset);
        return zoo;
    }

    private static @NotNull List<Stage<?, ?>> pets() {
        return List.of(FieldTransform.of("pets"), DeserializeTransform.of(DataType.list(DataTypes.STRING)));
    }

    private static @NotNull RotateTransform<JsonObject, String> rotation() {
        return RotateTransform.of(DataTypes.JSON_OBJECT, DataTypes.STRING, pets(), List.of(FieldTransform.of("offset"), AsIntTransform.of()));
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson(ZOO))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataTypes.JSON_OBJECT))
            .stage(rotation())
            .build();
    }

    @Test
    @DisplayName("The element at the offset comes first")
    void offsetElementComesFirst() {
        assertThat(rotation().execute(this.ctx, zoo(2)), contains("C", "D", "E", "F", "A", "B"));
    }

    @Test
    @DisplayName("A negative offset rotates the other way")
    void negativeOffsetRotatesBack() {
        assertThat(rotation().execute(this.ctx, zoo(-1)), contains("F", "A", "B", "C", "D", "E"));
    }

    @Test
    @DisplayName("An offset of the list's length leaves it as it is")
    void offsetOfLengthIsIdentity() {
        assertThat(rotation().execute(this.ctx, zoo(6)), contains("A", "B", "C", "D", "E", "F"));
    }

    @Test
    @DisplayName("An offset past the list's length wraps around")
    void offsetWraps() {
        assertThat(rotation().execute(this.ctx, zoo(8)), contains("C", "D", "E", "F", "A", "B"));
    }

    @Test
    @DisplayName("An offset of zero leaves the list as it is")
    void zeroOffsetIsIdentity() {
        assertThat(rotation().execute(this.ctx, zoo(0)), contains("A", "B", "C", "D", "E", "F"));
    }

    @Test
    @DisplayName("An empty list stays empty")
    void emptyStaysEmpty() {
        JsonObject input = JsonParser.parseString("{\"pets\":[],\"offset\":3}").getAsJsonObject();
        assertThat(rotation().execute(this.ctx, input), is(empty()));
    }

    @Test
    @DisplayName("A null list rejects with null")
    void nullListRejects() {
        JsonObject input = JsonParser.parseString("{\"offset\":3}").getAsJsonObject();
        assertThat(rotation().execute(this.ctx, input), is(nullValue()));
    }

    @Test
    @DisplayName("A null offset rejects with null")
    void nullOffsetRejects() {
        JsonObject input = JsonParser.parseString("{\"pets\":[\"A\"]}").getAsJsonObject();
        assertThat(rotation().execute(this.ctx, input), is(nullValue()));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(rotation().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A null element is carried to its rotated position")
    void nullElementCarried() {
        RotateTransform<List<String>, String> stage = RotateTransform.of(
            DataType.list(DataTypes.STRING), DataTypes.STRING,
            List.of(ReverseTransform.of(DataTypes.STRING), ReverseTransform.of(DataTypes.STRING)),
            List.of(FirstCollect.of(DataTypes.STRING), LengthTransform.of())
        );
        assertThat(stage.execute(this.ctx, Arrays.asList("a", null, "bb")), contains(null, "bb", "a"));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<String> result = rotation().execute(this.ctx, zoo(1));
        assertThrows(UnsupportedOperationException.class, () -> result.add("G"));
    }

    @Test
    @DisplayName("The output type is the element list type")
    void outputTypeIsElementList() {
        assertThat(rotation().outputType().label(), is(equalTo("List<STRING>")));
    }

    @Test
    @DisplayName("of rejects a list body that does not yield the element list type")
    void rejectsMistypedList() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> RotateTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.INT, pets(), List.of(FieldTransform.of("offset"), AsIntTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid RotateTransform list body"));
    }

    @Test
    @DisplayName("of rejects an offset body that does not yield INT")
    void rejectsMistypedOffset() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> RotateTransform.of(
            DataTypes.JSON_OBJECT, DataTypes.STRING, pets(), List.of(FieldTransform.of("offset"), AsStringTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid RotateTransform offset body"));
    }

    @Test
    @DisplayName("of rejects an empty offset body")
    void rejectsEmptyOffset() {
        assertThrows(IllegalArgumentException.class, () -> RotateTransform.of(DataTypes.JSON_OBJECT, DataTypes.STRING, pets(), List.of()));
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
    @DisplayName("A wire offset body that does not yield INT fails at load")
    void wireMistypedOffsetFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a,b\"},"
            + "{\"kind\":\"TRANSFORM_ROTATE\",\"inputType\":\"STRING\",\"elementType\":\"STRING\","
            + "\"list\":[{\"kind\":\"TRANSFORM_SPLIT\",\"regex\":\",\"}],\"offset\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid RotateTransform offset body"));
    }

}
