package dev.sbs.dataflow.stage.transform.encoding;

import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class EncodingTransformsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Base64 encode/decode round-trips a string")
    void base64RoundTrip() {
        String encoded = Base64EncodeTransform.of().execute(this.ctx, "hello world");
        assertThat(encoded, is(equalTo("aGVsbG8gd29ybGQ=")));
        assertThat(Base64DecodeTransform.of().execute(this.ctx, encoded), is(equalTo("hello world")));
    }

    @Test
    @DisplayName("Base64 decode of garbage returns null")
    void base64DecodeBadInput() {
        assertThat(Base64DecodeTransform.of().execute(this.ctx, "###"), is(nullValue()));
    }

    @Test
    @DisplayName("URL encode/decode round-trips reserved characters")
    void urlRoundTrip() {
        String encoded = UrlEncodeTransform.of().execute(this.ctx, "a b/c?d");
        assertThat(encoded, is(equalTo("a+b%2Fc%3Fd")));
        assertThat(UrlDecodeTransform.of().execute(this.ctx, encoded), is(equalTo("a b/c?d")));
    }

    @Test
    @DisplayName("URL decode of malformed escape returns null")
    void urlDecodeBadInput() {
        assertThat(UrlDecodeTransform.of().execute(this.ctx, "%ZZ"), is(nullValue()));
    }

}
