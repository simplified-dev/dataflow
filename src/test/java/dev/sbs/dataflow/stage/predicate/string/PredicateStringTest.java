package dev.sbs.dataflow.stage.predicate.string;

import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PredicateStringTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("Contains")
    void contains() {
        assertThat(StringContainsPredicate.of("foo").execute(this.ctx, "barfoobaz"), is(true));
        assertThat(StringContainsPredicate.of("foo").execute(this.ctx, "barbaz"), is(false));
        assertThat(StringContainsPredicate.of("foo").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("StartsWith")
    void startsWith() {
        assertThat(StringStartsWithPredicate.of("foo").execute(this.ctx, "foobar"), is(true));
        assertThat(StringStartsWithPredicate.of("foo").execute(this.ctx, "barfoo"), is(false));
    }

    @Test
    @DisplayName("EndsWith")
    void endsWith() {
        assertThat(StringEndsWithPredicate.of("bar").execute(this.ctx, "foobar"), is(true));
        assertThat(StringEndsWithPredicate.of("bar").execute(this.ctx, "barfoo"), is(false));
    }

    @Test
    @DisplayName("Equals")
    void equalsTo() {
        assertThat(StringEqualsPredicate.of("foo").execute(this.ctx, "foo"), is(true));
        assertThat(StringEqualsPredicate.of("foo").execute(this.ctx, "Foo"), is(false));
        assertThat(StringEqualsPredicate.of("foo").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Matches regex")
    void matches() {
        assertThat(StringMatchesPredicate.of("\\d+").execute(this.ctx, "abc123"), is(true));
        assertThat(StringMatchesPredicate.of("\\d+").execute(this.ctx, "abcdef"), is(false));
    }

    @Test
    @DisplayName("NonEmpty")
    void nonEmpty() {
        assertThat(StringNonEmptyPredicate.of().execute(this.ctx, "x"), is(true));
        assertThat(StringNonEmptyPredicate.of().execute(this.ctx, ""), is(false));
        assertThat(StringNonEmptyPredicate.of().execute(this.ctx, null), is(false));
    }

}
