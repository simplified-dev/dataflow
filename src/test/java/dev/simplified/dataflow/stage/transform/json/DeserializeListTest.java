package dev.simplified.dataflow.stage.transform.json;

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

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link DeserializeTransform} with a {@code List<X>} output over a Basic {@code X}.
 */
class DeserializeListTest {

    private static <T> @Nullable List<T> read(@NotNull DataType<T> element, @NotNull String json) {
        DeserializeTransform<JsonElement, List<T>> stage = DeserializeTransform.of(DataType.list(element));
        return stage.execute(PipelineContext.defaults(), JsonParser.parseString(json));
    }

    @Test
    @DisplayName("A List of a Basic type is accepted as the output")
    void acceptsListOfBasic() {
        assertThat(DeserializeTransform.of(DataType.list(DataTypes.INT)).outputType(), is(equalTo(DataType.list(DataTypes.INT))));
    }

    @Test
    @DisplayName("A List of a List is rejected at build time")
    void rejectsNestedList() {
        DataType<?> nested = DataType.list(DataType.list(DataTypes.STRING));
        assertThrows(IllegalArgumentException.class, () -> DeserializeTransform.of(nested));
    }

    @Test
    @DisplayName("An array becomes a list in array order")
    void arrayBecomesOrderedList() {
        assertThat(read(DataTypes.INT, "[3,1,2]"), contains(3, 1, 2));
    }

    @Test
    @DisplayName("An empty array becomes an empty list")
    void emptyArrayBecomesEmptyList() {
        assertThat(read(DataTypes.STRING, "[]"), is(empty()));
    }

    @Test
    @DisplayName("A JSON null element is dropped")
    void nullElementDropped() {
        assertThat(read(DataTypes.INT, "[1,null,3]"), contains(1, 3));
    }

    @Test
    @DisplayName("An element that does not convert is dropped")
    void unconvertibleElementDropped() {
        assertThat(read(DataTypes.STRING, "[\"a\",{\"x\":1},\"b\",[2]]"), contains("a", "b"));
    }

    @Test
    @DisplayName("A primitive element is dropped from a List<JSON_OBJECT>")
    void primitiveDroppedFromObjectList() {
        List<JsonObject> rows = read(DataTypes.JSON_OBJECT, "[{\"a\":1},2,{\"b\":2}]");
        assertThat(rows, hasSize(2));
    }

    @Test
    @DisplayName("An element that is not a number is dropped from a List<INT>")
    void nonNumberDroppedFromIntList() {
        assertThat(read(DataTypes.INT, "[1,\"x\",3]"), contains(1, 3));
    }

    @Test
    @DisplayName("A non-array input rejects with null")
    void nonArrayRejects() {
        assertThat(read(DataTypes.INT, "{\"a\":1}"), is(nullValue()));
    }

    @Test
    @DisplayName("A primitive input rejects with null")
    void primitiveRejects() {
        assertThat(read(DataTypes.INT, "7"), is(nullValue()));
    }

    @Test
    @DisplayName("A null input returns null")
    void nullInputReturnsNull() {
        DeserializeTransform<JsonElement, List<Integer>> stage = DeserializeTransform.of(DataType.list(DataTypes.INT));
        assertThat(stage.execute(PipelineContext.defaults(), null), is(nullValue()));
    }

    @Test
    @DisplayName("The list is unmodifiable")
    void listIsUnmodifiable() {
        List<Integer> values = read(DataTypes.INT, "[1]");
        assertThrows(UnsupportedOperationException.class, () -> values.add(2));
    }

    @Test
    @DisplayName("A Basic output still converts the whole input")
    void basicOutputUnchanged() {
        DeserializeTransform<JsonElement, String> stage = DeserializeTransform.of(DataTypes.STRING);
        assertThat(stage.execute(PipelineContext.defaults(), JsonParser.parseString("\"x\"")), is(equalTo("x")));
    }

    @Test
    @DisplayName("A List output round-trips on the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A List output round-trips on the wire to the same output")
    void wireRoundTripExecutes() {
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline())).execute(), is(equalTo(List.of(1, 2))));
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.rawJson("[1,null,2]"))
            .stage(ParseJsonTransform.of())
            .stage(DeserializeTransform.of(DataType.list(DataTypes.INT)))
            .build();
    }

}
