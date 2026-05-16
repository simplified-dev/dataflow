package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollectStagesTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("First returns the first element or null on empty")
    void first() {
        assertThat(FirstCollect.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b")), is(equalTo("a")));
        assertThat(FirstCollect.of(DataTypes.STRING).execute(this.ctx, List.of()), is(nullValue()));
        assertThat(FirstCollect.of(DataTypes.STRING).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Last returns the last element or null on empty")
    void last() {
        assertThat(LastCollect.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b")), is(equalTo("b")));
        assertThat(LastCollect.of(DataTypes.STRING).execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("List returns the input unchanged")
    void list() {
        List<String> in = List.of("a", "b", "c");
        assertThat(ListCollect.of(DataTypes.STRING).execute(this.ctx, in), contains("a", "b", "c"));
    }

    @Test
    @DisplayName("Set drops duplicates by equals")
    void set() {
        Set<String> result = SetCollect.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "a"));
        assertThat(result, containsInAnyOrder("a", "b"));
    }

    @Test
    @DisplayName("Join produces a separator-delimited string")
    void join() {
        assertThat(JoinCollect.of(", ").execute(this.ctx, List.of("a", "b", "c")), is(equalTo("a, b, c")));
    }

    @Test
    @DisplayName("Nth returns the element at the configured index, or null when out of range")
    void nth() {
        List<String> in = List.of("a", "b", "c");
        assertThat(NthCollect.of(DataTypes.STRING, 0).execute(this.ctx, in), is(equalTo("a")));
        assertThat(NthCollect.of(DataTypes.STRING, 2).execute(this.ctx, in), is(equalTo("c")));
        assertThat(NthCollect.of(DataTypes.STRING, 5).execute(this.ctx, in), is(nullValue()));
        assertThat(NthCollect.of(DataTypes.STRING, 0).execute(this.ctx, List.of()), is(nullValue()));
        assertThat(NthCollect.of(DataTypes.STRING, 0).execute(this.ctx, null), is(nullValue()));
        assertThat(NthCollect.of(DataTypes.STRING, -3).index(), is(equalTo(0)));
    }

    @Test
    @DisplayName("SubList returns the half-open [from, to) sub-range, clamping out-of-range bounds")
    void subList() {
        List<String> in = List.of("a", "b", "c");
        assertThat(SubListCollect.of(DataTypes.STRING, 0, 2).execute(this.ctx, in), contains("a", "b"));
        assertThat(SubListCollect.of(DataTypes.STRING, 1, null).execute(this.ctx, in), contains("b", "c"));
        assertThat(SubListCollect.of(DataTypes.STRING, 5, 10).execute(this.ctx, in), is(empty()));
        assertThat(SubListCollect.of(DataTypes.STRING, 0, 5).execute(this.ctx, null), is(nullValue()));
        assertThrows(
            IllegalArgumentException.class,
            () -> SubListCollect.of(DataTypes.STRING, 2, 1)
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> SubListCollect.of(DataTypes.STRING, 0, 2).execute(this.ctx, in).add("x")
        );
    }

}
