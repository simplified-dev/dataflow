package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.predicate.numeric.IntLessThanPredicate;
import dev.simplified.dataflow.stage.transform.string.ReplaceTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IterateTransformTest {

    private static final PipelineContext CTX = PipelineContext.defaults();

    @Test
    @DisplayName("with no condition the body runs until it stops changing the value")
    void untilStable() {
        IterateTransform<String> stage = IterateTransform.of(DataTypes.STRING, List.of(ReplaceTransform.of("aa", "a")), null, 100);

        assertThat(stage.execute(CTX, "aaaaaaaab"), equalTo("ab"));
        assertThat(stage.execute(CTX, "b"), equalTo("b"));
    }

    @Test
    @DisplayName("with a condition the body runs while it holds")
    void whileCondition() {
        IterateTransform<Integer> stage = IterateTransform.of(
            DataTypes.INT, List.of(ArithmeticIntTransform.of("ADD", 2)), List.of(IntLessThanPredicate.of(7)), 100
        );

        assertThat(stage.execute(CTX, 0), equalTo(8));
        assertThat(stage.execute(CTX, 9), equalTo(9));
    }

    @Test
    @DisplayName("a loop still changing the value after maxIterations passes fails the run")
    void maxIterationsReached() {
        IterateTransform<Integer> stage = IterateTransform.of(DataTypes.INT, List.of(ArithmeticIntTransform.of("ADD", 1)), null, 5);

        assertThrows(IllegalStateException.class, () -> stage.execute(CTX, 0));
    }

    @Test
    @DisplayName("a pass that outgrows the value budget fails the run")
    void valueBudget() {
        IterateTransform<String> stage = IterateTransform.of(DataTypes.STRING, List.of(ReplaceTransform.of("^(.*)$", "$1$1")), null, 100);

        assertThrows(IllegalStateException.class, () -> stage.execute(CTX, "x"));
    }

    @Test
    @DisplayName("a null input and a body that rejects the value both yield null")
    void nulls() {
        IterateTransform<String> stage = IterateTransform.of(DataTypes.STRING, List.of(ReplaceTransform.of("a", "b")), null, 10);

        assertThat(stage.execute(CTX, null), nullValue());
    }

    @Test
    @DisplayName("refuses a pass cap outside 1 to the ceiling and a body of another type")
    void refusals() {
        assertThrows(IllegalArgumentException.class, () -> IterateTransform.of(DataTypes.STRING, List.of(ReplaceTransform.of("a", "b")), null, 0));
        assertThrows(IllegalArgumentException.class, () -> IterateTransform.of(DataTypes.STRING, List.of(ReplaceTransform.of("a", "b")), null, IterateTransform.MAX_ITERATIONS_CEILING + 1));
        assertThrows(IllegalArgumentException.class, () -> IterateTransform.of(DataTypes.INT, List.of(ReplaceTransform.of("a", "b")), null, 10));
    }

    @Test
    @DisplayName("round-trips through the wire format with its while body")
    void wireRoundTrip() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"0\"},"
            + "{\"kind\":\"TRANSFORM_ITERATE\",\"type\":\"INT\",\"maxIterations\":100,"
            + "\"body\":[{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"ADD\",\"operand\":3}],"
            + "\"while\":[{\"kind\":\"PREDICATE_INT_LESS_THAN\",\"threshold\":10}]}]";

        assertThat(PipelineGson.fromJson(json).execute(CTX), equalTo(12));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(json)).contains("\"while\""), equalTo(true));
    }

}
