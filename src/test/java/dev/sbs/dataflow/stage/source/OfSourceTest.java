package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OfSourceTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("OfSource(STRING) emits the configured string verbatim")
    void ofString() {
        assertThat(OfSource.of(DataTypes.STRING, "hello").execute(this.ctx, null), is(equalTo("hello")));
    }

    @Test
    @DisplayName("OfSource(INT) parses the configured string as int")
    void ofInt() {
        assertThat(OfSource.of(DataTypes.INT, "42").execute(this.ctx, null), is(equalTo(42)));
    }

    @Test
    @DisplayName("OfSource(LONG) parses the configured string as long")
    void ofLong() {
        assertThat(OfSource.of(DataTypes.LONG, "9999999999").execute(this.ctx, null), is(equalTo(9999999999L)));
    }

    @Test
    @DisplayName("OfSource(DOUBLE) parses the configured string as double")
    void ofDouble() {
        assertThat(OfSource.of(DataTypes.DOUBLE, "3.14").execute(this.ctx, null), is(equalTo(3.14)));
    }

    @Test
    @DisplayName("OfSource(BOOLEAN) parses the configured string as boolean")
    void ofBoolean() {
        assertThat(OfSource.of(DataTypes.BOOLEAN, "true").execute(this.ctx, null), is(equalTo(true)));
        assertThat(OfSource.of(DataTypes.BOOLEAN, "false").execute(this.ctx, null), is(equalTo(false)));
    }

    @Test
    @DisplayName("OfSource(RAW_HTML) emits the configured body verbatim")
    void ofRawHtml() {
        String body = "<html><body>hi</body></html>";
        assertThat(OfSource.of(DataTypes.RAW_HTML, body).execute(this.ctx, null), is(equalTo(body)));
    }

    @Test
    @DisplayName("OfSource rejects structured types at build time")
    void ofRejectsStructuredTypes() {
        assertThrows(IllegalArgumentException.class, () -> OfSource.of(DataTypes.DOM_NODE, "<p/>"));
        assertThrows(IllegalArgumentException.class, () -> OfSource.of(DataTypes.JSON_OBJECT, "{}"));
    }

    @Test
    @DisplayName("OfSource(INT) rejects unparseable values at build time")
    void ofRejectsUnparseable() {
        assertThrows(NumberFormatException.class, () -> OfSource.of(DataTypes.INT, "not-a-number"));
    }

}
