package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ArithmeticIntTransform}, {@link ArithmeticLongTransform},
 * {@link ArithmeticFloatTransform} and {@link ArithmeticDoubleTransform}: the input is the left
 * operand, the configured operand the right, and each stage round-trips through the wire.
 */
class ArithmeticTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<DataPipeline<?>> wirePipelines() {
        return List.of(
            DataPipeline.builder()
                .source(LiteralSource.integerVal(7))
                .stage(ArithmeticIntTransform.of("SUBTRACT", 10))
                .build(),
            DataPipeline.builder()
                .source(LiteralSource.longVal(-7L))
                .stage(ArithmeticLongTransform.of("MODULO", 5L))
                .build(),
            DataPipeline.builder()
                .source(LiteralSource.floatVal(3.35f))
                .stage(ArithmeticFloatTransform.of("DIVIDE", 0.336))
                .build(),
            DataPipeline.builder()
                .source(LiteralSource.doubleVal(3.35))
                .stage(ArithmeticDoubleTransform.of("MULTIPLY", 0.24))
                .build()
        );
    }

    @Test
    @DisplayName("int ADD adds the operand to the input")
    void intAdd() {
        assertThat(ArithmeticIntTransform.of("ADD", 5).execute(this.ctx, 7), is(equalTo(12)));
    }

    @Test
    @DisplayName("int SUBTRACT takes the operand from the input")
    void intSubtractOrder() {
        assertThat(ArithmeticIntTransform.of("SUBTRACT", 10).execute(this.ctx, 7), is(equalTo(-3)));
    }

    @Test
    @DisplayName("int DIVIDE divides the input by the operand, truncating toward zero")
    void intDivideOrder() {
        assertThat(ArithmeticIntTransform.of("DIVIDE", 2).execute(this.ctx, -7), is(equalTo(-3)));
    }

    @Test
    @DisplayName("int MODULO of a negative input by a positive operand is not negative")
    void intModulo() {
        assertThat(ArithmeticIntTransform.of("MODULO", 7).execute(this.ctx, -2), is(equalTo(5)));
    }

    @Test
    @DisplayName("int null input rejects with null")
    void intNullInput() {
        assertThat(ArithmeticIntTransform.of("ADD", 1).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("int DIVIDE by a zero operand rejects with null")
    void intZeroOperand() {
        assertThat(ArithmeticIntTransform.of("DIVIDE", 0).execute(this.ctx, 7), is(nullValue()));
    }

    @Test
    @DisplayName("int MULTIPLY overflow throws")
    void intOverflowThrows() {
        ArithmeticIntTransform stage = ArithmeticIntTransform.of("MULTIPLY", 2);
        assertThrows(ArithmeticException.class, () -> stage.execute(this.ctx, Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("int of refuses an unknown operator")
    void intRefusesUnknownOperator() {
        assertThrows(IllegalArgumentException.class, () -> ArithmeticIntTransform.of("POWER", 2));
    }

    @Test
    @DisplayName("int config carries the operator name as given")
    void intConfigKeepsOperatorName() {
        assertThat(ArithmeticIntTransform.of("MODULO", 3).config().getString("operator"), is(equalTo("MODULO")));
    }

    @Test
    @DisplayName("int operator() answers the resolved operator")
    void intResolvesOperator() {
        assertThat(ArithmeticIntTransform.of("MODULO", 3).operator(), is(ArithmeticOperator.MODULO));
    }

    @Test
    @DisplayName("long SUBTRACT takes the operand from the input")
    void longSubtractOrder() {
        assertThat(ArithmeticLongTransform.of("SUBTRACT", 10L).execute(this.ctx, 7L), is(equalTo(-3L)));
    }

    @Test
    @DisplayName("long MULTIPLY computes past the int range")
    void longMultiply() {
        assertThat(ArithmeticLongTransform.of("MULTIPLY", 1_000L).execute(this.ctx, 3_000_000_000L), is(equalTo(3_000_000_000_000L)));
    }

    @Test
    @DisplayName("long null input rejects with null")
    void longNullInput() {
        assertThat(ArithmeticLongTransform.of("ADD", 1L).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("long MODULO by a zero operand rejects with null")
    void longZeroOperand() {
        assertThat(ArithmeticLongTransform.of("MODULO", 0L).execute(this.ctx, 7L), is(nullValue()));
    }

    @Test
    @DisplayName("long ADD overflow throws")
    void longOverflowThrows() {
        ArithmeticLongTransform stage = ArithmeticLongTransform.of("ADD", 1L);
        assertThrows(ArithmeticException.class, () -> stage.execute(this.ctx, Long.MAX_VALUE));
    }

    @Test
    @DisplayName("float DIVIDE divides the input by the operand")
    void floatDivideOrder() {
        assertThat(ArithmeticFloatTransform.of("DIVIDE", 4.0).execute(this.ctx, 1f), is(equalTo(0.25f)));
    }

    @Test
    @DisplayName("float arithmetic runs in float precision on the narrowed operand")
    void floatUsesNarrowedOperand() {
        assertThat(ArithmeticFloatTransform.of("ADD", 0.1).execute(this.ctx, 0.2f), is(equalTo(0.2f + 0.1f)));
    }

    @Test
    @DisplayName("float null input rejects with null")
    void floatNullInput() {
        assertThat(ArithmeticFloatTransform.of("ADD", 1.0).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("float DIVIDE by a zero operand rejects with null")
    void floatZeroOperand() {
        assertThat(ArithmeticFloatTransform.of("DIVIDE", 0.0).execute(this.ctx, 7f), is(nullValue()));
    }

    @Test
    @DisplayName("float result overflowing to infinity rejects with null")
    void floatInfiniteResult() {
        assertThat(ArithmeticFloatTransform.of("MULTIPLY", 10.0).execute(this.ctx, Float.MAX_VALUE), is(nullValue()));
    }

    @Test
    @DisplayName("float of refuses an operand outside the float range")
    void floatRefusesOutOfRangeOperand() {
        assertThrows(IllegalArgumentException.class, () -> ArithmeticFloatTransform.of("ADD", 1e300));
    }

    @Test
    @DisplayName("float of refuses a NaN operand")
    void floatRefusesNaNOperand() {
        assertThrows(IllegalArgumentException.class, () -> ArithmeticFloatTransform.of("ADD", Double.NaN));
    }

    @Test
    @DisplayName("float config carries the operand as given, not narrowed")
    void floatConfigKeepsRawOperand() {
        assertThat(ArithmeticFloatTransform.of("ADD", 0.1).config().getDouble("operand"), is(equalTo(0.1)));
    }

    @Test
    @DisplayName("double SUBTRACT takes the operand from the input")
    void doubleSubtractOrder() {
        assertThat(ArithmeticDoubleTransform.of("SUBTRACT", 4.0).execute(this.ctx, 1.5), is(equalTo(-2.5)));
    }

    @Test
    @DisplayName("double MODULO of a negative input by a positive operand is not negative")
    void doubleModulo() {
        assertThat(ArithmeticDoubleTransform.of("MODULO", 4.0).execute(this.ctx, -1.5), is(equalTo(2.5)));
    }

    @Test
    @DisplayName("double null input rejects with null")
    void doubleNullInput() {
        assertThat(ArithmeticDoubleTransform.of("ADD", 1.0).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("double DIVIDE by a zero operand rejects with null")
    void doubleZeroOperand() {
        assertThat(ArithmeticDoubleTransform.of("DIVIDE", 0.0).execute(this.ctx, 7.0), is(nullValue()));
    }

    @Test
    @DisplayName("double NaN input rejects with null")
    void doubleNaNInput() {
        assertThat(ArithmeticDoubleTransform.of("ADD", 1.0).execute(this.ctx, Double.NaN), is(nullValue()));
    }

    @Test
    @DisplayName("double of refuses an infinite operand")
    void doubleRefusesInfiniteOperand() {
        assertThrows(IllegalArgumentException.class, () -> ArithmeticDoubleTransform.of("MULTIPLY", Double.POSITIVE_INFINITY));
    }

    @Test
    @DisplayName("A wire operator that names no ArithmeticOperator fails at load")
    void wireUnknownOperatorFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"POWER\",\"operand\":2}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @DisplayName("A wire INT operand with a fraction fails at load rather than truncating")
    void wireFractionalIntOperandFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"MULTIPLY\",\"operand\":2.5}]";
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> PipelineGson.fromJson(json));
        assertThat(thrown.getMessage(), startsWith("Field 'operand' holds '2.5'"));
    }

    @Test
    @DisplayName("A wire INT operand past the int range fails at load rather than wrapping")
    void wireOutOfRangeIntOperandFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"ADD\",\"operand\":3000000000}]";
        assertThrows(IllegalArgumentException.class, () -> PipelineGson.fromJson(json));
    }

    @Test
    @DisplayName("A wire INT operand written with a zero fraction loads as its integer")
    void wireIntegralDecimalIntOperandLoads() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"MULTIPLY\",\"operand\":2.0}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo(14)));
    }

    @Test
    @DisplayName("A wire LONG operand with a fraction fails at load rather than truncating")
    void wireFractionalLongOperandFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"LONG\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_LONG\",\"operator\":\"ADD\",\"operand\":9.5}]";
        assertThrows(IllegalArgumentException.class, () -> PipelineGson.fromJson(json));
    }

    @Test
    @DisplayName("A wire LONG operand past the long range fails at load rather than wrapping")
    void wireOutOfRangeLongOperandFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"LONG\",\"value\":\"7\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_LONG\",\"operator\":\"ADD\",\"operand\":10000000000000000000}]";
        assertThrows(IllegalArgumentException.class, () -> PipelineGson.fromJson(json));
    }

    @Test
    @DisplayName("A wire stage reads its operator and operand keys")
    void wireReadsKeys() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"DOUBLE\",\"value\":\"3.35\"},"
            + "{\"kind\":\"TRANSFORM_ARITHMETIC_DOUBLE\",\"operator\":\"DIVIDE\",\"operand\":0.5}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo(6.7)));
    }

    @Test
    @DisplayName("The wire form names the operator and operand")
    void wireFormNamesKeys() {
        DataPipeline<Integer> pipeline = DataPipeline.builder()
            .source(LiteralSource.integerVal(7))
            .stage(ArithmeticIntTransform.of("SUBTRACT", 10))
            .build();
        assertThat(
            PipelineGson.toJson(pipeline),
            is(equalTo("[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"7\"},"
                + "{\"kind\":\"TRANSFORM_ARITHMETIC_INT\",\"operator\":\"SUBTRACT\",\"operand\":10}]"))
        );
    }

    @TestFactory
    @DisplayName("Each arithmetic stage round-trips through the wire to the same config and output")
    Stream<DynamicTest> wireRoundTrips() {
        return wirePipelines().stream().flatMap(pipeline -> {
            String json = PipelineGson.toJson(pipeline);
            DataPipeline<?> rebuilt = PipelineGson.fromJson(json);
            String id = pipeline.stages().getLast().kindId();
            return Stream.of(
                DynamicTest.dynamicTest(id + " kind", () -> assertThat(rebuilt.stages().getLast().kindId(), is(equalTo(id)))),
                DynamicTest.dynamicTest(id + " config", () -> assertThat(PipelineGson.toJson(rebuilt), is(equalTo(json)))),
                DynamicTest.dynamicTest(id + " output", () -> assertThat(rebuilt.execute(), is(equalTo(pipeline.execute()))))
            );
        });
    }

}
