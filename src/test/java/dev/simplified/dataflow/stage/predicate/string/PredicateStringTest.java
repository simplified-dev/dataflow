package dev.simplified.dataflow.stage.predicate.string;

import dev.simplified.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PredicateStringTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Contains")
    void contains() {
        assertThat(ContainsPredicate.of("foo").execute(this.ctx, "barfoobaz"), is(true));
        assertThat(ContainsPredicate.of("foo").execute(this.ctx, "barbaz"), is(false));
        assertThat(ContainsPredicate.of("foo").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("StartsWith")
    void startsWith() {
        assertThat(StartsWithPredicate.of("foo").execute(this.ctx, "foobar"), is(true));
        assertThat(StartsWithPredicate.of("foo").execute(this.ctx, "barfoo"), is(false));
    }

    @Test
    @DisplayName("EndsWith")
    void endsWith() {
        assertThat(EndsWithPredicate.of("bar").execute(this.ctx, "foobar"), is(true));
        assertThat(EndsWithPredicate.of("bar").execute(this.ctx, "barfoo"), is(false));
    }

    @Test
    @DisplayName("Equals")
    void equalsTo() {
        assertThat(EqualsPredicate.of("foo").execute(this.ctx, "foo"), is(true));
        assertThat(EqualsPredicate.of("foo").execute(this.ctx, "Foo"), is(false));
        assertThat(EqualsPredicate.of("foo").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Matches regex")
    void matches() {
        assertThat(MatchesPredicate.of("\\d+").execute(this.ctx, "abc123"), is(true));
        assertThat(MatchesPredicate.of("\\d+").execute(this.ctx, "abcdef"), is(false));
    }

    @Test
    @DisplayName("NonEmpty")
    void nonEmpty() {
        assertThat(NonEmptyPredicate.of().execute(this.ctx, "x"), is(true));
        assertThat(NonEmptyPredicate.of().execute(this.ctx, ""), is(false));
        assertThat(NonEmptyPredicate.of().execute(this.ctx, null), is(false));
    }

}
