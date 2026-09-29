package dev.simplified.dataflow.stage.transform.encoding;

import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class JsonUnescapeTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    private @Nullable String decode(@NotNull String input) {
        return JsonUnescapeTransform.of().execute(this.ctx, input);
    }

    private static @NotNull DataPipeline<String> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.text("Terry\\u0027s \\\"+4\\\" 50% \\u003cb\\u003e \\u00e9"))
            .stage(JsonUnescapeTransform.of())
            .build();
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(JsonUnescapeTransform.of().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A string with no escape is returned unchanged")
    void noEscape() {
        assertThat(decode("plain text"), is(equalTo("plain text")));
    }

    @Test
    @DisplayName("The eight single-character escapes decode")
    void singleCharacterEscapes() {
        assertThat(decode("a\\\"b\\\\c\\/d\\be\\ff\\ng\\rh\\ti"), is(equalTo("a\"b\\c/d\be\ff\ng\rh\ti")));
    }

    @Test
    @DisplayName("A four-hex-digit escape decodes to its character")
    void unicodeEscape() {
        assertThat(decode("Terry\\u0027s"), is(equalTo("Terry's")));
    }

    @Test
    @DisplayName("Upper-case hex digits decode")
    void upperCaseHex() {
        assertThat(decode("caf\\u00E9"), is(equalTo("caf\u00e9")));
    }

    @Test
    @DisplayName("An escape at or above U+0080 decodes")
    void escapeAboveAscii() {
        assertThat(decode("a\\u2028b"), is(equalTo("a\u2028b")));
    }

    @Test
    @DisplayName("Two escapes forming a surrogate pair decode to one supplementary character")
    void surrogatePairJoins() {
        assertThat(decode("\\ud83d\\ude00").codePointAt(0), is(equalTo(0x1F600)));
    }

    @Test
    @DisplayName("A lone surrogate escape decodes to the lone surrogate")
    void loneSurrogate() {
        assertThat(decode("\\ud83d"), is(equalTo("\uD83D")));
    }

    @Test
    @DisplayName("A quote no backslash escapes passes through")
    void unescapedQuotePassesThrough() {
        assertThat(decode("say \"hi\" \\n"), is(equalTo("say \"hi\" \n")));
    }

    @Test
    @DisplayName("A backslash that ends the input rejects with null")
    void trailingBackslash() {
        assertThat(decode("abc\\"), is(nullValue()));
    }

    @Test
    @DisplayName("A backslash before a character no escape starts with rejects with null")
    void unknownEscape() {
        assertThat(decode("a\\xb"), is(nullValue()));
    }

    @Test
    @DisplayName("A u escape running out of input before four hex digits rejects with null")
    void shortUnicodeEscapeAtEnd() {
        assertThat(decode("a\\u12"), is(nullValue()));
    }

    @Test
    @DisplayName("A u escape with a non-hex digit among its four rejects with null")
    void nonHexUnicodeEscape() {
        assertThat(decode("a\\u12G4"), is(nullValue()));
    }

    @Test
    @DisplayName("A u escape with non-ASCII digits rejects with null")
    void nonAsciiDigits() {
        assertThat(decode("a\\u\u0661\u0662\u0663\u0664"), is(nullValue()));
    }

    @Test
    @DisplayName("Decoding agrees with a JSON parser reading the same text as a string")
    void agreesWithJsonParser() {
        String escaped = "\\u0027\\u003c\\u003e\\u0026\\u003d \\\"q\\\" \\\\ \\/ \\ud83d\\ude00 \\u00e9 \\t";
        assertThat(decode(escaped), is(equalTo(JsonParser.parseString("\"" + escaped + "\"").getAsString())));
    }

    @Test
    @DisplayName("A pipeline round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips to the same output")
    void wireRoundTripExecutes() {
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline())).execute(), is(equalTo(pipeline().execute())));
    }

    @Test
    @DisplayName("The round-tripped pipeline decodes the payload")
    void wireRoundTripDecodes() {
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline())).execute(), is(equalTo("Terry's \"+4\" 50% <b> \u00e9")));
    }

}
