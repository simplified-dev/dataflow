package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonDeserializeTransformTest {

    record Sample(String name, int score, Map<String, Integer> stats) {}

    private static final DataType<Sample> SAMPLE_TYPE = new DataType.Basic<>(Sample.class, "GSON_SAMPLE");

    @Test
    @DisplayName("Deserialises a JsonObject into a record with nested Map<String,Integer>")
    void deserialisesRecord() {
        JsonObject input = new JsonObject();
        input.add("name", new JsonPrimitive("Dark Claymore"));
        input.add("score", new JsonPrimitive(500));
        JsonObject stats = new JsonObject();
        stats.add("strength", new JsonPrimitive(220));
        stats.add("crit", new JsonPrimitive(175));
        input.add("stats", stats);

        Sample result = JsonDeserializeTransform.of(SAMPLE_TYPE)
            .execute(PipelineContext.defaults(), input);

        assertThat(result, is(notNullValue()));
        assertThat(result.name(), is(equalTo("Dark Claymore")));
        assertThat(result.score(), is(equalTo(500)));
        assertThat(result.stats().get("strength"), is(equalTo(220)));
        assertThat(result.stats().get("crit"), is(equalTo(175)));
    }

    @Test
    @DisplayName("Null and JsonNull inputs return null")
    void nullInputReturnsNull() {
        Sample result = JsonDeserializeTransform.of(SAMPLE_TYPE)
            .execute(PipelineContext.defaults(), null);
        assertThat(result, is(nullValue()));
    }

    @Test
    @DisplayName("Rejects parameterised DataType at build time")
    void rejectsListType() {
        DataType<?> listType = DataType.list(DataTypes.STRING);
        assertThrows(IllegalArgumentException.class,
            () -> JsonDeserializeTransform.of(listType));
    }

    @Test
    @DisplayName("Output type is the declared target; default input is JSON_ELEMENT")
    void outputTypeMatches() {
        JsonDeserializeTransform<?, Sample> stage = JsonDeserializeTransform.of(SAMPLE_TYPE);
        assertThat(stage.outputType(), is(SAMPLE_TYPE));
        assertThat(stage.inputType(), is(DataTypes.JSON_ELEMENT));
    }

    @Test
    @DisplayName("Explicit JSON_OBJECT input is accepted")
    void acceptsJsonObjectInput() {
        JsonDeserializeTransform<?, Sample> stage =
            JsonDeserializeTransform.of(DataTypes.JSON_OBJECT, SAMPLE_TYPE);
        assertThat(stage.inputType(), is(DataTypes.JSON_OBJECT));
    }


}
