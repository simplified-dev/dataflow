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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class CollectStagesTest {

    private final PipelineContext ctx = PipelineContext.empty();

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

}
