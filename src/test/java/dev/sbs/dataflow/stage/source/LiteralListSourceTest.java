package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LiteralListSourceTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("LiteralListSource(STRING) emits the parsed array")
    void ofStringList() {
        List<String> result = LiteralListSource.of(DataTypes.STRING, "[\"a\",\"b\",\"c\"]").execute(this.ctx, null);
        assertThat(result, contains("a", "b", "c"));
    }

    @Test
    @DisplayName("LiteralListSource(INT) parses numeric arrays")
    void ofIntList() {
        List<Integer> result = LiteralListSource.of(DataTypes.INT, "[1,2,3]").execute(this.ctx, null);
        assertThat(result, contains(1, 2, 3));
    }

    @Test
    @DisplayName("LiteralListSource handles empty arrays")
    void ofEmptyList() {
        List<String> result = LiteralListSource.of(DataTypes.STRING, "[]").execute(this.ctx, null);
        assertThat(result, is(List.of()));
    }

    @Test
    @DisplayName("LiteralListSource rejects malformed JSON")
    void ofRejectsBadJson() {
        assertThrows(IllegalArgumentException.class, () -> LiteralListSource.of(DataTypes.STRING, "[not-json]"));
        assertThrows(IllegalArgumentException.class, () -> LiteralListSource.of(DataTypes.STRING, "not-an-array"));
    }

    @Test
    @DisplayName("LiteralListSource rejects structured element types")
    void ofRejectsStructuredElementTypes() {
        assertThrows(IllegalArgumentException.class, () -> LiteralListSource.of(DataTypes.DOM_NODE, "[]"));
    }

    @Test
    @DisplayName("Varargs factories produce the matching DataType + value list")
    void varargsFactoriesProduceMatchingValues() {
        assertThat(LiteralListSource.strings("a", "b", "c").execute(this.ctx, null), contains("a", "b", "c"));
        assertThat(LiteralListSource.integers(1, 2, 3).execute(this.ctx, null),      contains(1, 2, 3));
        assertThat(LiteralListSource.longs(1L, 2L, 3L).execute(this.ctx, null),      contains(1L, 2L, 3L));
        assertThat(LiteralListSource.floats(1.5f, 2.5f).execute(this.ctx, null),     contains(1.5f, 2.5f));
        assertThat(LiteralListSource.doubles(1.5, 2.5).execute(this.ctx, null),      contains(1.5, 2.5));
        assertThat(LiteralListSource.booleans(true, false).execute(this.ctx, null),  contains(true, false));
    }

    @Test
    @DisplayName("strings(...) escapes embedded quotes and special chars via Gson")
    void stringsFactoryEscapes() {
        List<String> result = LiteralListSource.strings("a\"b", "c\\d").execute(this.ctx, null);
        assertThat(result, contains("a\"b", "c\\d"));
    }

    @Test
    @DisplayName("Empty varargs produce empty lists")
    void varargsFactoriesEmpty() {
        assertThat(LiteralListSource.strings().execute(this.ctx, null), is(List.of()));
        assertThat(LiteralListSource.integers().execute(this.ctx, null), is(List.of()));
    }

}
