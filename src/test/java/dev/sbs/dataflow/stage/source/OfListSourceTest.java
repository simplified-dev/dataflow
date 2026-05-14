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

class OfListSourceTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("OfListSource(STRING) emits the parsed array")
    void ofStringList() {
        List<String> result = OfListSource.of(DataTypes.STRING, "[\"a\",\"b\",\"c\"]").execute(this.ctx, null);
        assertThat(result, contains("a", "b", "c"));
    }

    @Test
    @DisplayName("OfListSource(INT) parses numeric arrays")
    void ofIntList() {
        List<Integer> result = OfListSource.of(DataTypes.INT, "[1,2,3]").execute(this.ctx, null);
        assertThat(result, contains(1, 2, 3));
    }

    @Test
    @DisplayName("OfListSource handles empty arrays")
    void ofEmptyList() {
        List<String> result = OfListSource.of(DataTypes.STRING, "[]").execute(this.ctx, null);
        assertThat(result, is(List.of()));
    }

    @Test
    @DisplayName("OfListSource rejects malformed JSON")
    void ofRejectsBadJson() {
        assertThrows(IllegalArgumentException.class, () -> OfListSource.of(DataTypes.STRING, "[not-json]"));
        assertThrows(IllegalArgumentException.class, () -> OfListSource.of(DataTypes.STRING, "not-an-array"));
    }

    @Test
    @DisplayName("OfListSource rejects structured element types")
    void ofRejectsStructuredElementTypes() {
        assertThrows(IllegalArgumentException.class, () -> OfListSource.of(DataTypes.DOM_NODE, "[]"));
    }

}
