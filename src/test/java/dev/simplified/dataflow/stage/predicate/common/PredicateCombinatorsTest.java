package dev.simplified.dataflow.stage.predicate.common;

import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.simplified.dataflow.stage.predicate.numeric.IntLessThanPredicate;
import dev.simplified.dataflow.stage.predicate.string.ContainsPredicate;
import dev.simplified.dataflow.stage.predicate.string.NonEmptyPredicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PredicateCombinatorsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("NotNull on any element type")
    void notNull() {
        assertThat(NotNullPredicate.of(DataTypes.STRING).execute(this.ctx, "x"), is(true));
        assertThat(NotNullPredicate.of(DataTypes.STRING).execute(this.ctx, null), is(false));
        assertThat(NotNullPredicate.of(DataTypes.INT).execute(this.ctx, 42), is(true));
    }

    @Test
    @DisplayName("Not inverts the input boolean; null passes through")
    void not() {
        assertThat(NotPredicate.of().execute(this.ctx, true), is(false));
        assertThat(NotPredicate.of().execute(this.ctx, false), is(true));
        assertThat(NotPredicate.of().execute(this.ctx, null), is(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    @DisplayName("And returns true only when every body is true; empty map is vacuously true")
    void and() {
        Map<String, List<? extends Stage<?, ?>>> bodies = new LinkedHashMap<>();
        bodies.put("hasFoo", List.of(ContainsPredicate.of("foo")));
        bodies.put("nonEmpty", List.of(NonEmptyPredicate.of()));
        AndPredicate<String> stage = AndPredicate.of(DataTypes.STRING, bodies);
        assertThat(stage.execute(this.ctx, "foobar"), is(true));
        assertThat(stage.execute(this.ctx, "bar"), is(false));
        assertThat(stage.execute(this.ctx, ""), is(false));

        AndPredicate<String> empty = AndPredicate.of(DataTypes.STRING, Map.of());
        assertThat(empty.execute(this.ctx, "anything"), is(true));
    }

    @Test
    @DisplayName("And returns false on first body that fails")
    void andFalseOnFirstFalse() {
        Map<String, List<? extends Stage<?, ?>>> bodies = new LinkedHashMap<>();
        bodies.put("over1000", List.of(IntGreaterThanPredicate.of(1000)));
        bodies.put("under1000", List.of(IntLessThanPredicate.of(1000)));
        AndPredicate<Integer> stage = AndPredicate.of(DataTypes.INT, bodies);
        assertThat(stage.execute(this.ctx, 5), is(false));
        assertThat(stage.execute(this.ctx, 2000), is(false));
    }

    @Test
    @DisplayName("Or returns true when any body is true; empty map is vacuously false")
    void or() {
        Map<String, List<? extends Stage<?, ?>>> bodies = new LinkedHashMap<>();
        bodies.put("hasFoo", List.of(ContainsPredicate.of("foo")));
        bodies.put("hasBar", List.of(ContainsPredicate.of("bar")));
        OrPredicate<String> stage = OrPredicate.of(DataTypes.STRING, bodies);
        assertThat(stage.execute(this.ctx, "barbaz"), is(true));
        assertThat(stage.execute(this.ctx, "qux"), is(false));

        OrPredicate<String> empty = OrPredicate.of(DataTypes.STRING, Map.of());
        assertThat(empty.execute(this.ctx, "anything"), is(false));
    }

    @Test
    @DisplayName("Or returns true on first body that matches")
    void orTrueOnFirstTrue() {
        Map<String, List<? extends Stage<?, ?>>> bodies = new LinkedHashMap<>();
        bodies.put("over10", List.of(IntGreaterThanPredicate.of(10)));
        bodies.put("under0", List.of(IntLessThanPredicate.of(0)));
        OrPredicate<Integer> stage = OrPredicate.of(DataTypes.INT, bodies);
        assertThat(stage.execute(this.ctx, 5), is(false));
        assertThat(stage.execute(this.ctx, 50), is(true));
        assertThat(stage.execute(this.ctx, -5), is(true));
    }

    @Test
    @DisplayName("And/Or reject body whose final stage does not produce BOOLEAN")
    void rejectsNonBooleanBody() {
        assertThrows(IllegalArgumentException.class, () -> AndPredicate.of(
            DataTypes.STRING,
            Map.of("bad", List.of())
        ));
        assertThrows(IllegalArgumentException.class, () -> OrPredicate.of(
            DataTypes.STRING,
            Map.of("bad", List.of(ContainsPredicate.of("foo"), ContainsPredicate.of("bar")))
        ));
    }

}
