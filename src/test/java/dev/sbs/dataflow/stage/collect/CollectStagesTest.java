package dev.sbs.dataflow.stage.collect;

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
        assertThat(CollectFirst.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b")), is(equalTo("a")));
        assertThat(CollectFirst.of(DataTypes.STRING).execute(this.ctx, List.of()), is(nullValue()));
        assertThat(CollectFirst.of(DataTypes.STRING).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Last returns the last element or null on empty")
    void last() {
        assertThat(CollectLast.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b")), is(equalTo("b")));
        assertThat(CollectLast.of(DataTypes.STRING).execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("List returns the input unchanged")
    void list() {
        List<String> in = List.of("a", "b", "c");
        assertThat(CollectList.of(DataTypes.STRING).execute(this.ctx, in), contains("a", "b", "c"));
    }

    @Test
    @DisplayName("Set drops duplicates by equals")
    void set() {
        Set<String> result = CollectSet.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "a"));
        assertThat(result, containsInAnyOrder("a", "b"));
    }

    @Test
    @DisplayName("Join produces a separator-delimited string")
    void join() {
        assertThat(CollectJoin.of(", ").execute(this.ctx, List.of("a", "b", "c")), is(equalTo("a, b, c")));
    }

}
