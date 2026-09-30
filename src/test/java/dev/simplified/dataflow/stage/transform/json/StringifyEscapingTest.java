package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers {@link StringifyTransform} writing HTML-significant characters as themselves, and object
 * members holding JSON {@code null} as {@code null}. Before, it serialised with a default
 * {@code Gson}, which wrote {@code '}, {@code <}, {@code >}, {@code &} and {@code =} as
 * backslash-u escapes, and it left every null member out of the text.
 */
class StringifyEscapingTest {

    @Test
    @DisplayName("HTML-significant characters inside strings are written as themselves")
    void htmlCharactersUnescaped() {
        JsonObject input = new JsonObject();
        input.addProperty("t", "<b>'x' & y=z</b>");
        assertThat(StringifyTransform.of().execute(PipelineContext.defaults(), input),
            is(equalTo("{\"t\":\"<b>'x' & y=z</b>\"}")));
    }

    @Test
    @DisplayName("The output parses back to the same element")
    void outputParsesBack() {
        JsonObject input = new JsonObject();
        input.addProperty("t", "<b>'x' & y=z</b>");
        String text = StringifyTransform.of().execute(PipelineContext.defaults(), input);
        assertThat(JsonParser.parseString(text), is(equalTo(input)));
    }

    @Test
    @DisplayName("Quotes and backslashes are still escaped")
    void jsonEscapesKept() {
        JsonObject input = new JsonObject();
        input.addProperty("t", "a\"b\\c");
        assertThat(StringifyTransform.of().execute(PipelineContext.defaults(), input),
            is(equalTo("{\"t\":\"a\\\"b\\\\c\"}")));
    }

    @Test
    @DisplayName("A member whose value is JSON null is written rather than left out")
    void nullMemberKept() {
        JsonElement input = JsonParser.parseString("{\"a\":null,\"b\":1}");
        assertThat(StringifyTransform.of().execute(PipelineContext.defaults(), input), is(equalTo("{\"a\":null,\"b\":1}")));
    }

    @Test
    @DisplayName("A nested member whose value is JSON null is written rather than left out")
    void nestedNullMemberKept() {
        JsonElement input = JsonParser.parseString("{\"c\":{\"d\":null},\"e\":[null,{\"f\":null}]}");
        assertThat(StringifyTransform.of().execute(PipelineContext.defaults(), input),
            is(equalTo("{\"c\":{\"d\":null},\"e\":[null,{\"f\":null}]}")));
    }

    @Test
    @DisplayName("An object holding JSON null members parses back to the same element")
    void nullMembersParseBack() {
        JsonElement input = JsonParser.parseString("{\"a\":null,\"c\":{\"d\":null}}");
        String text = StringifyTransform.of().execute(PipelineContext.defaults(), input);
        assertThat(JsonParser.parseString(text), is(equalTo(input)));
    }

}
