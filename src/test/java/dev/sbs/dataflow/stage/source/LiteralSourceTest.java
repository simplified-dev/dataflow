package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LiteralSourceTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("LiteralSource(STRING) emits the configured string verbatim")
    void ofString() {
        assertThat(LiteralSource.of(DataTypes.STRING, "hello").execute(this.ctx, null), is(equalTo("hello")));
    }

    @Test
    @DisplayName("LiteralSource(INT) parses the configured string as int")
    void ofInt() {
        assertThat(LiteralSource.of(DataTypes.INT, "42").execute(this.ctx, null), is(equalTo(42)));
    }

    @Test
    @DisplayName("LiteralSource(LONG) parses the configured string as long")
    void ofLong() {
        assertThat(LiteralSource.of(DataTypes.LONG, "9999999999").execute(this.ctx, null), is(equalTo(9999999999L)));
    }

    @Test
    @DisplayName("LiteralSource(DOUBLE) parses the configured string as double")
    void ofDouble() {
        assertThat(LiteralSource.of(DataTypes.DOUBLE, "3.14").execute(this.ctx, null), is(equalTo(3.14)));
    }

    @Test
    @DisplayName("LiteralSource(BOOLEAN) parses the configured string as boolean")
    void ofBoolean() {
        assertThat(LiteralSource.of(DataTypes.BOOLEAN, "true").execute(this.ctx, null), is(equalTo(true)));
        assertThat(LiteralSource.of(DataTypes.BOOLEAN, "false").execute(this.ctx, null), is(equalTo(false)));
    }

    @Test
    @DisplayName("LiteralSource(RAW_HTML) emits the configured body verbatim")
    void ofRawHtml() {
        String body = "<html><body>hi</body></html>";
        assertThat(LiteralSource.of(DataTypes.RAW_HTML, body).execute(this.ctx, null), is(equalTo(body)));
    }

    @Test
    @DisplayName("LiteralSource rejects structured types at build time")
    void ofRejectsStructuredTypes() {
        assertThrows(IllegalArgumentException.class, () -> LiteralSource.of(DataTypes.DOM_NODE, "<p/>"));
        assertThrows(IllegalArgumentException.class, () -> LiteralSource.of(DataTypes.JSON_OBJECT, "{}"));
    }

    @Test
    @DisplayName("LiteralSource(INT) rejects unparseable values at build time")
    void ofRejectsUnparseable() {
        assertThrows(NumberFormatException.class, () -> LiteralSource.of(DataTypes.INT, "not-a-number"));
    }

}
