package dev.simplified.dataflow.stage.transform.encoding;

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

class HtmlDecodeTransformTest {

    private static final @NotNull String MSGNW = "&#123;&#9;name &#61; &#39;Hilda&#39;, cost &#61; 5 &#125;";

    private final PipelineContext ctx = PipelineContext.defaults();

    private @Nullable String decode(@NotNull String input) {
        return HtmlDecodeTransform.of().execute(this.ctx, input);
    }

    private static @NotNull DataPipeline<String> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.text(MSGNW))
            .stage(HtmlDecodeTransform.of())
            .build();
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(HtmlDecodeTransform.of().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A string with no reference is returned unchanged")
    void noReference() {
        assertThat(decode("plain text"), is(equalTo("plain text")));
    }

    @Test
    @DisplayName("Decimal references decode, including a tab")
    void decimalReferences() {
        assertThat(decode(MSGNW), is(equalTo("{\tname = 'Hilda', cost = 5 }")));
    }

    @Test
    @DisplayName("Hexadecimal references decode in either case")
    void hexReferences() {
        assertThat(decode("&#x7B;&#X7d;"), is(equalTo("{}")));
    }

    @Test
    @DisplayName("Named references decode")
    void namedReferences() {
        assertThat(decode("&lt;b&gt; &quot;x&quot; &amp; &eacute;"), is(equalTo("<b> \"x\" & \u00e9")));
    }

    @Test
    @DisplayName("A name HTML does not define stays as written")
    void unknownNameStays() {
        assertThat(decode("a &bogus; b"), is(equalTo("a &bogus; b")));
    }

    @Test
    @DisplayName("A bare ampersand stays as written")
    void bareAmpersandStays() {
        assertThat(decode("salt & pepper"), is(equalTo("salt & pepper")));
    }

    @Test
    @DisplayName("Decoded text is not decoded a second time")
    void noDoubleDecode() {
        assertThat(decode("&#38;#61;"), is(equalTo("&#61;")));
    }

    @Test
    @DisplayName("A legacy name without its semicolon decodes, as in HTML text")
    void legacyNameWithoutSemicolon() {
        assertThat(decode("a &amp b"), is(equalTo("a & b")));
    }

    @Test
    @DisplayName("A supplementary character reference decodes to one code point")
    void supplementaryReference() {
        assertThat(decode("&#128512;").codePointAt(0), is(equalTo(0x1F600)));
    }

    @Test
    @DisplayName("A zero reference decodes to U+0000, as jsoup decodes it")
    void zeroReferenceDecodesToNul() {
        assertThat(decode("&#0;"), is(equalTo("\u0000")));
    }

    @Test
    @DisplayName("A surrogate reference decodes to that lone surrogate, as jsoup decodes it")
    void surrogateReferenceDecodesToLoneSurrogate() {
        assertThat(decode("&#xD800;"), is(equalTo("\uD800")));
    }

    @Test
    @DisplayName("Two surrogate references forming a pair decode to one supplementary character, as jsoup decodes them")
    void surrogatePairReferencesJoin() {
        assertThat(decode("&#xD83D;&#xDE00;").codePoints().toArray(), is(equalTo(new int[] { 0x1F600 })));
    }

    @Test
    @DisplayName("A reference past jsoup's digit limit decodes only its leading digits")
    void overlongReferenceDecodesLeadingDigits() {
        assertThat(decode("&#" + "0".repeat(2045) + "65;"), is(equalTo("\u00065;")));
    }

    @Test
    @DisplayName("Line breaks, carriage returns and NUL characters pass through unchanged")
    void controlCharactersPassThrough() {
        assertThat(decode("a\r\nb\rc\u0000d&#10;"), is(equalTo("a\r\nb\rc\u0000d\n")));
    }

    @Test
    @DisplayName("A reference decodes wherever it falls in a long input")
    void referencesAcrossLongInput() {
        String padding = "x".repeat(32760);
        StringBuilder input = new StringBuilder();
        StringBuilder expected = new StringBuilder();

        for (int shift = 0; shift < 16; shift++) {
            input.append(padding, 0, 32760 - shift).append("&#123;&amp;&#x7D;");
            expected.append(padding, 0, 32760 - shift).append("{&}");
        }

        assertThat(decode(input.toString()), is(equalTo(expected.toString())));
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

}
