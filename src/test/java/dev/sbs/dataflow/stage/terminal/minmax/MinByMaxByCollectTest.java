package dev.sbs.dataflow.stage.terminal.minmax;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MinByMaxByCollectTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("MinBy picks the element with the smallest key")
    void minByShortestString() {
        MinByCollect<String, Integer> collect = MinByCollect.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        assertThat(collect.execute(this.ctx, List.of("ccc", "a", "bb")), is(equalTo("a")));
    }

    @Test
    @DisplayName("MaxBy picks the element with the largest key")
    void maxByLongestString() {
        MaxByCollect<String, Integer> collect = MaxByCollect.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        assertThat(collect.execute(this.ctx, List.of("ccc", "a", "bb")), is(equalTo("ccc")));
    }

    @Test
    @DisplayName("First wins on ties")
    void firstWinsOnTies() {
        MinByCollect<String, Integer> collect = MinByCollect.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        // both "ab" and "cd" have length 2; the first one wins.
        assertThat(collect.execute(this.ctx, List.of("ab", "cd")), is(equalTo("ab")));
    }

    @Test
    @DisplayName("Returns null for empty input")
    void emptyInput() {
        MinByCollect<String, Integer> collect = MinByCollect.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        assertThat(collect.execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("Skips elements whose body yields null; returns null when every key is null")
    void skipsNullKeys() {
        MinByCollect<String, Integer> collect = MinByCollect.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        // mixed list: nulls are skipped, "a" is the only real element
        assertThat(collect.execute(this.ctx, Arrays.asList(null, "a", null)), is(equalTo("a")));
        // all nulls: result is null
        assertThat(collect.execute(this.ctx, Arrays.asList(null, null)), is(nullValue()));
    }

    @Test
    @DisplayName("MinBy / MaxBy reject unsupported key types")
    void rejectsUnsupportedKey() {
        assertThrows(IllegalArgumentException.class, () -> MinByCollect.of(
            DataTypes.STRING, DataTypes.BOOLEAN, List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> MaxByCollect.of(
            DataTypes.STRING, DataTypes.BOOLEAN, List.of()
        ));
    }

    @Test
    @DisplayName("MinBy / MaxBy reject bodies that do not produce the key type")
    void rejectsBadBody() {
        assertThrows(IllegalArgumentException.class, () -> MinByCollect.of(
            DataTypes.STRING, DataTypes.INT, List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> MaxByCollect.of(
            DataTypes.STRING, DataTypes.INT, List.of()
        ));
    }

}
