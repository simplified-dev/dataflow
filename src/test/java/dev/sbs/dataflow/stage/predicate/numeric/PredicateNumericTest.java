package dev.sbs.dataflow.stage.predicate.numeric;

import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PredicateNumericTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Int >")
    void intGreaterThan() {
        assertThat(IntGreaterThanPredicate.of(10).execute(this.ctx, 15), is(true));
        assertThat(IntGreaterThanPredicate.of(10).execute(this.ctx, 10), is(false));
        assertThat(IntGreaterThanPredicate.of(10).execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Int <")
    void intLessThan() {
        assertThat(IntLessThanPredicate.of(10).execute(this.ctx, 5), is(true));
        assertThat(IntLessThanPredicate.of(10).execute(this.ctx, 10), is(false));
    }

    @Test
    @DisplayName("Int in [min, max]")
    void intInRange() {
        assertThat(IntInRangePredicate.of(0, 10).execute(this.ctx, 5), is(true));
        assertThat(IntInRangePredicate.of(0, 10).execute(this.ctx, 0), is(true));
        assertThat(IntInRangePredicate.of(0, 10).execute(this.ctx, 10), is(true));
        assertThat(IntInRangePredicate.of(0, 10).execute(this.ctx, -1), is(false));
        assertThat(IntInRangePredicate.of(0, 10).execute(this.ctx, 11), is(false));
    }

    @Test
    @DisplayName("Long >")
    void longGreaterThan() {
        assertThat(LongGreaterThanPredicate.of(10L).execute(this.ctx, 15L), is(true));
        assertThat(LongGreaterThanPredicate.of(10L).execute(this.ctx, 10L), is(false));
    }

    @Test
    @DisplayName("Long <")
    void longLessThan() {
        assertThat(LongLessThanPredicate.of(10L).execute(this.ctx, 5L), is(true));
        assertThat(LongLessThanPredicate.of(10L).execute(this.ctx, 10L), is(false));
    }

    @Test
    @DisplayName("Long in [min, max]")
    void longInRange() {
        assertThat(LongInRangePredicate.of(0L, 10L).execute(this.ctx, 5L), is(true));
        assertThat(LongInRangePredicate.of(0L, 10L).execute(this.ctx, 11L), is(false));
    }

    @Test
    @DisplayName("Double >")
    void doubleGreaterThan() {
        assertThat(DoubleGreaterThanPredicate.of(1.5).execute(this.ctx, 2.0), is(true));
        assertThat(DoubleGreaterThanPredicate.of(1.5).execute(this.ctx, 1.5), is(false));
    }

    @Test
    @DisplayName("Double <")
    void doubleLessThan() {
        assertThat(DoubleLessThanPredicate.of(1.5).execute(this.ctx, 1.0), is(true));
        assertThat(DoubleLessThanPredicate.of(1.5).execute(this.ctx, 1.5), is(false));
    }

    @Test
    @DisplayName("Double in [min, max]")
    void doubleInRange() {
        assertThat(DoubleInRangePredicate.of(0.0, 10.0).execute(this.ctx, 5.5), is(true));
        assertThat(DoubleInRangePredicate.of(0.0, 10.0).execute(this.ctx, -0.5), is(false));
    }

}
