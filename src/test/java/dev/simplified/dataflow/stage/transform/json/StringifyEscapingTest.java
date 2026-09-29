package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers {@link StringifyTransform} writing HTML-significant characters as themselves. Before,
 * it serialised with a default {@code Gson}, which wrote {@code '}, {@code <}, {@code >},
 * {@code &} and {@code =} as backslash-u escapes.
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

}
