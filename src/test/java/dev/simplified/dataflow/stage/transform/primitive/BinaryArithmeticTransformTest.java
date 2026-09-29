package dev.simplified.dataflow.stage.transform.primitive;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.AsIntTransform;
import dev.simplified.dataflow.stage.transform.json.PathTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link BinaryArithmeticIntTransform}, {@link BinaryArithmeticLongTransform},
 * {@link BinaryArithmeticFloatTransform} and {@link BinaryArithmeticDoubleTransform}. Most cases
 * read a {@code "left,right"} string, the left body taking the text before the comma and the
 * right body the text after it.
 */
class BinaryArithmeticTransformTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"7,2\"}";

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull List<Stage<?, ?>> before(@NotNull Stage<?, ?> parse) {
        return List.of(RegexExtractTransform.of("^[^,]+"), parse);
    }

    private static @NotNull List<Stage<?, ?>> after(@NotNull Stage<?, ?> parse) {
        return List.of(RegexExtractTransform.of("[^,]+$"), parse);
    }

    private static @NotNull BinaryArithmeticIntTransform<String> ints(@NotNull String operator) {
        return BinaryArithmeticIntTransform.of(DataTypes.STRING, operator, before(ParseIntTransform.of()), after(ParseIntTransform.of()));
    }

    private static @NotNull BinaryArithmeticLongTransform<String> longs(@NotNull String operator) {
        return BinaryArithmeticLongTransform.of(DataTypes.STRING, operator, before(ParseLongTransform.of()), after(ParseLongTransform.of()));
    }

    private static @NotNull BinaryArithmeticFloatTransform<String> floats(@NotNull String operator) {
        return BinaryArithmeticFloatTransform.of(DataTypes.STRING, operator, before(ParseFloatTransform.of()), after(ParseFloatTransform.of()));
    }

    private static @NotNull BinaryArithmeticDoubleTransform<String> doubles(@NotNull String operator) {
        return BinaryArithmeticDoubleTransform.of(DataTypes.STRING, operator, before(ParseDoubleTransform.of()), after(ParseDoubleTransform.of()));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<DataPipeline<?>> wirePipelines() {
        return List.of(
            DataPipeline.builder().source(LiteralSource.text("7,10")).stage(ints("SUBTRACT")).build(),
            DataPipeline.builder().source(LiteralSource.text("-7,5")).stage(longs("MODULO")).build(),
            DataPipeline.builder().source(LiteralSource.text("7,2")).stage(floats("DIVIDE")).build(),
            DataPipeline.builder().source(LiteralSource.text("1.5,4")).stage(doubles("MULTIPLY")).build()
        );
    }

    @Test
    @DisplayName("int SUBTRACT takes the right body's value from the left body's")
    void intSubtractOrder() {
        assertThat(ints("SUBTRACT").execute(this.ctx, "7,10"), is(equalTo(-3)));
    }

    @Test
    @DisplayName("int DIVIDE divides the left value by the right, truncating toward zero")
    void intDivideOrder() {
        assertThat(ints("DIVIDE").execute(this.ctx, "-7,2"), is(equalTo(-3)));
    }

    @Test
    @DisplayName("int MODULO of a negative left value by a positive right value is not negative")
    void intModulo() {
        assertThat(ints("MODULO").execute(this.ctx, "-2,7"), is(equalTo(5)));
    }

    @Test
    @DisplayName("int null input rejects with null")
    void intNullInput() {
        assertThat(ints("ADD").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("int left body yielding null rejects with null")
    void intLeftNullRejects() {
        assertThat(ints("ADD").execute(this.ctx, "x,2"), is(nullValue()));
    }

    @Test
    @DisplayName("int right body yielding null rejects with null")
    void intRightNullRejects() {
        assertThat(ints("ADD").execute(this.ctx, "7,x"), is(nullValue()));
    }

    @Test
    @DisplayName("int right body does not run when the left body yields null")
    void intRightSkippedAfterLeftNull() {
        ParseIntTransform watched = ParseIntTransform.of();
        BinaryArithmeticIntTransform<String> stage = BinaryArithmeticIntTransform.of(
            DataTypes.STRING, "ADD", before(ParseIntTransform.of()), after(watched)
        );
        AtomicInteger runs = new AtomicInteger();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((ran, output) -> {
                if (ran == watched) runs.incrementAndGet();
            })
            .build();

        stage.execute(counting, "x,2");

        assertThat(runs.get(), is(0));
    }

    @Test
    @DisplayName("int DIVIDE by a zero right value rejects with null")
    void intZeroDivisor() {
        assertThat(ints("DIVIDE").execute(this.ctx, "7,0"), is(nullValue()));
    }

    @Test
    @DisplayName("int ADD overflow throws")
    void intOverflowThrows() {
        BinaryArithmeticIntTransform<String> stage = ints("ADD");
        assertThrows(ArithmeticException.class, () -> stage.execute(this.ctx, "2147483647,1"));
    }

    @Test
    @DisplayName("int bodies read two fields of one JSON row")
    void intOverJsonRow() {
        BinaryArithmeticIntTransform<JsonElement> stage = BinaryArithmeticIntTransform.of(
            DataTypes.JSON_ELEMENT,
            "MODULO",
            List.of(PathTransform.of("a"), AsIntTransform.of()),
            List.of(PathTransform.of("b"), AsIntTransform.of())
        );
        assertThat(stage.execute(this.ctx, JsonParser.parseString("{\"a\":9,\"b\":4}")), is(equalTo(1)));
    }

    @Test
    @DisplayName("int of refuses an unknown operator")
    void intRefusesUnknownOperator() {
        assertThrows(IllegalArgumentException.class, () -> ints("POWER"));
    }

    @Test
    @DisplayName("int of refuses a left body that does not produce INT")
    void intRefusesMistypedLeft() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> BinaryArithmeticIntTransform.of(
            DataTypes.STRING, "ADD", List.of(RegexExtractTransform.of("^[^,]+")), after(ParseIntTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid BinaryArithmeticIntTransform body 'left'"));
    }

    @Test
    @DisplayName("int of refuses a right body whose type chain breaks")
    void intRefusesBrokenRight() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> BinaryArithmeticIntTransform.of(
            DataTypes.STRING, "ADD", before(ParseIntTransform.of()), List.of(LengthTransform.of(), ParseIntTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid BinaryArithmeticIntTransform body 'right'"));
    }

    @Test
    @DisplayName("int of refuses an empty body")
    void intRefusesEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> BinaryArithmeticIntTransform.of(
            DataTypes.STRING, "ADD", before(ParseIntTransform.of()), List.of()
        ));
    }

    @Test
    @DisplayName("int of refuses a body opening with a source")
    void intRefusesSourceInBody() {
        assertThrows(IllegalArgumentException.class, () -> BinaryArithmeticIntTransform.of(
            DataTypes.STRING, "ADD", List.of(LiteralSource.integerVal(1)), after(ParseIntTransform.of())
        ));
    }

    @Test
    @DisplayName("int config carries the operator name as given")
    void intConfigKeepsOperatorName() {
        assertThat(ints("MULTIPLY").config().getString("operator"), is(equalTo("MULTIPLY")));
    }

    @Test
    @DisplayName("long MULTIPLY computes past the int range")
    void longMultiply() {
        assertThat(longs("MULTIPLY").execute(this.ctx, "3000000000,1000"), is(equalTo(3_000_000_000_000L)));
    }

    @Test
    @DisplayName("long SUBTRACT takes the right body's value from the left body's")
    void longSubtractOrder() {
        assertThat(longs("SUBTRACT").execute(this.ctx, "7,10"), is(equalTo(-3L)));
    }

    @Test
    @DisplayName("long null input rejects with null")
    void longNullInput() {
        assertThat(longs("ADD").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("long right body yielding null rejects with null")
    void longRightNullRejects() {
        assertThat(longs("ADD").execute(this.ctx, "7,x"), is(nullValue()));
    }

    @Test
    @DisplayName("long MODULO by a zero right value rejects with null")
    void longZeroDivisor() {
        assertThat(longs("MODULO").execute(this.ctx, "7,0"), is(nullValue()));
    }

    @Test
    @DisplayName("long MULTIPLY overflow throws")
    void longOverflowThrows() {
        BinaryArithmeticLongTransform<String> stage = longs("MULTIPLY");
        assertThrows(ArithmeticException.class, () -> stage.execute(this.ctx, "9223372036854775807,2"));
    }

    @Test
    @DisplayName("float DIVIDE divides the left value by the right, keeping the fraction")
    void floatDivideOrder() {
        assertThat(floats("DIVIDE").execute(this.ctx, "7,2"), is(equalTo(3.5f)));
    }

    @Test
    @DisplayName("float null input rejects with null")
    void floatNullInput() {
        assertThat(floats("ADD").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("float left body yielding null rejects with null")
    void floatLeftNullRejects() {
        assertThat(floats("ADD").execute(this.ctx, "x,2"), is(nullValue()));
    }

    @Test
    @DisplayName("float result overflowing to infinity rejects with null")
    void floatInfiniteResult() {
        assertThat(floats("MULTIPLY").execute(this.ctx, "3e38,10"), is(nullValue()));
    }

    @Test
    @DisplayName("float DIVIDE by a zero right value rejects with null")
    void floatZeroDivisor() {
        assertThat(floats("DIVIDE").execute(this.ctx, "7,0"), is(nullValue()));
    }

    @Test
    @DisplayName("double DIVIDE by a scaled right body divides by the scaled value")
    void doubleDivideByScaledRight() {
        BinaryArithmeticDoubleTransform<String> stage = BinaryArithmeticDoubleTransform.of(
            DataTypes.STRING,
            "DIVIDE",
            before(ParseDoubleTransform.of()),
            List.of(RegexExtractTransform.of("[^,]+$"), ParseDoubleTransform.of(), ArithmeticDoubleTransform.of("MULTIPLY", 0.24))
        );
        assertThat(stage.execute(this.ctx, "3.35,1.4"), is(closeTo(9.970238, 1e-6)));
    }

    @Test
    @DisplayName("double MODULO of a negative left value by a positive right value is not negative")
    void doubleModulo() {
        assertThat(doubles("MODULO").execute(this.ctx, "-1.5,4"), is(equalTo(2.5)));
    }

    @Test
    @DisplayName("double null input rejects with null")
    void doubleNullInput() {
        assertThat(doubles("ADD").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("double right body yielding null rejects with null")
    void doubleRightNullRejects() {
        assertThat(doubles("ADD").execute(this.ctx, "7,x"), is(nullValue()));
    }

    @Test
    @DisplayName("double DIVIDE by a zero right value rejects with null")
    void doubleZeroDivisor() {
        assertThat(doubles("DIVIDE").execute(this.ctx, "7,0"), is(nullValue()));
    }

    @Test
    @DisplayName("double NaN left value rejects with null")
    void doubleNaNRejects() {
        assertThat(doubles("ADD").execute(this.ctx, "NaN,1"), is(nullValue()));
    }

    @Test
    @DisplayName("A wire body that does not produce the stage's type fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_BINARY_ARITHMETIC_INT\",\"inputType\":\"STRING\",\"operator\":\"ADD\","
            + "\"left\":[{\"kind\":\"TRANSFORM_TRIM\"}],\"right\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid BinaryArithmeticIntTransform body 'left'"));
    }

    @Test
    @DisplayName("A wire stage reads its inputType, operator, left and right keys")
    void wireReadsKeys() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_BINARY_ARITHMETIC_INT\",\"inputType\":\"STRING\",\"operator\":\"MULTIPLY\","
            + "\"left\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}],\"right\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}]}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo(9)));
    }

    @TestFactory
    @DisplayName("Each binary arithmetic stage round-trips through the wire to the same config and output")
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
