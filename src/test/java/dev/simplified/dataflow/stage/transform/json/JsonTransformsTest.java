package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class JsonTransformsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("JsonAsString returns string primitives")
    void jsonAsString() {
        JsonElement primitive = JsonParser.parseString("\"hello\"");
        assertThat(AsStringTransform.of().execute(this.ctx, primitive), is(equalTo("hello")));
    }

    @Test
    @DisplayName("JsonAs* returns null on objects/arrays/null")
    void jsonAsNonPrimitive() {
        JsonObject obj = new JsonObject();
        JsonArray arr = new JsonArray();
        assertThat(AsStringTransform.of().execute(this.ctx, obj), is(nullValue()));
        assertThat(AsIntTransform.of().execute(this.ctx, arr), is(nullValue()));
    }

    @Test
    @DisplayName("JsonAs(Int|Long|Double|Boolean) on primitives")
    void jsonAsPrimitives() {
        assertThat(AsIntTransform.of().execute(this.ctx, JsonParser.parseString("42")), is(equalTo(42)));
        assertThat(AsLongTransform.of().execute(this.ctx, JsonParser.parseString("9999999999")), is(equalTo(9999999999L)));
        assertThat(AsDoubleTransform.of().execute(this.ctx, JsonParser.parseString("3.14")), is(equalTo(3.14)));
        assertThat(AsBooleanTransform.of().execute(this.ctx, JsonParser.parseString("true")), is(true));
    }

    @Test
    @DisplayName("JsonStringify reserialises a JsonElement")
    void jsonStringify() {
        JsonElement el = JsonParser.parseString("{\"a\":1}");
        assertThat(StringifyTransform.of().execute(this.ctx, el), is(equalTo("{\"a\":1}")));
    }

}
