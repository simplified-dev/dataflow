package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SortByTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Sort strings ascending by length")
    void sortByLengthAsc() {
        SortByTransform<String, Integer> sort = SortByTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            true,
            List.of(StringLengthTransform.of())
        );
        assertThat(sort.execute(this.ctx, List.of("ccc", "a", "bb")), contains("a", "bb", "ccc"));
    }

    @Test
    @DisplayName("Sort strings descending by length")
    void sortByLengthDesc() {
        SortByTransform<String, Integer> sort = SortByTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            false,
            List.of(StringLengthTransform.of())
        );
        assertThat(sort.execute(this.ctx, List.of("a", "ccc", "bb")), contains("ccc", "bb", "a"));
    }

    @Test
    @DisplayName("Elements whose body yields null are pushed to the end")
    void nullKeysGoToEnd() {
        // StringLengthTransform on a null element yields null; we feed a list containing null to trigger that path.
        SortByTransform<String, Integer> sort = SortByTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            true,
            List.of(StringLengthTransform.of())
        );
        List<String> result = sort.execute(this.ctx, Arrays.asList("bb", null, "a"));
        // "a" (len 1) and "bb" (len 2) sort naturally, null goes last regardless of direction.
        assertThat(result, contains("a", "bb", null));
    }

    @Test
    @DisplayName("Rejects unsupported key types at build time")
    void rejectsUnsupportedKey() {
        assertThrows(IllegalArgumentException.class, () -> SortByTransform.of(
            DataTypes.STRING,
            DataTypes.BOOLEAN,
            true,
            List.of()
        ));
    }

    @Test
    @DisplayName("Rejects body whose final output does not match the key type")
    void rejectsBadBody() {
        assertThrows(IllegalArgumentException.class, () -> SortByTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            true,
            List.of()  // empty body cannot produce INT
        ));
    }

}
