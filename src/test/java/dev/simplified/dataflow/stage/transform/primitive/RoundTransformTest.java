package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link RoundDoubleTransform} and {@link RoundFloatTransform}: decimal rounding at a
 * scale under a {@link RoundingMode}, {@link RoundingMode#HALF_UP HALF_UP} when none is configured,
 * and the wire round trip with the mode present and absent.
 */
class RoundTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<DataPipeline<?>> wirePipelines() {
        return List.of(
            DataPipeline.builder().source(LiteralSource.doubleVal(2.675)).stage(RoundDoubleTransform.of(2, "HALF_EVEN")).build(),
            DataPipeline.builder().source(LiteralSource.doubleVal(9.97)).stage(RoundDoubleTransform.of(0, null)).build(),
            DataPipeline.builder().source(LiteralSource.floatVal(2.45f)).stage(RoundFloatTransform.of(1, "HALF_EVEN")).build(),
            DataPipeline.builder().source(LiteralSource.floatVal(9.97f)).stage(RoundFloatTransform.of(0, null)).build()
        );
    }

    @Test
    @DisplayName("double rounds 9.97 at scale 0 to 10.0")
    void doubleRoundsToWhole() {
        assertThat(RoundDoubleTransform.of(0, null).execute(this.ctx, 9.97), is(equalTo(10.0)));
    }

    @Test
    @DisplayName("double rounds the decimal form, so 2.675 at scale 2 HALF_UP is 2.68")
    void doubleRoundsDecimalForm() {
        assertThat(RoundDoubleTransform.of(2, "HALF_UP").execute(this.ctx, 2.675), is(equalTo(2.68)));
    }

    @Test
    @DisplayName("double with no mode rounds a half up")
    void doubleDefaultsToHalfUp() {
        assertThat(RoundDoubleTransform.of(0, null).execute(this.ctx, 2.5), is(equalTo(3.0)));
    }

    @Test
    @DisplayName("double HALF_EVEN rounds a half to the even neighbour")
    void doubleHalfEven() {
        assertThat(RoundDoubleTransform.of(0, "HALF_EVEN").execute(this.ctx, 2.5), is(equalTo(2.0)));
    }

    @Test
    @DisplayName("double FLOOR rounds a negative value toward negative infinity")
    void doubleFloor() {
        assertThat(RoundDoubleTransform.of(1, "FLOOR").execute(this.ctx, -1.21), is(equalTo(-1.3)));
    }

    @Test
    @DisplayName("double negative scale rounds left of the decimal point")
    void doubleNegativeScale() {
        assertThat(RoundDoubleTransform.of(-2, null).execute(this.ctx, 1250.5), is(equalTo(1300.0)));
    }

    @Test
    @DisplayName("double value already within the scale is unchanged")
    void doubleWithinScaleUnchanged() {
        assertThat(RoundDoubleTransform.of(3, null).execute(this.ctx, 1.5), is(equalTo(1.5)));
    }

    @Test
    @DisplayName("double null input rejects with null")
    void doubleNullInput() {
        assertThat(RoundDoubleTransform.of(2, null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("double NaN input rejects with null")
    void doubleNaNInput() {
        assertThat(RoundDoubleTransform.of(2, null).execute(this.ctx, Double.NaN), is(nullValue()));
    }

    @Test
    @DisplayName("double infinite input rejects with null")
    void doubleInfiniteInput() {
        assertThat(RoundDoubleTransform.of(2, null).execute(this.ctx, Double.NEGATIVE_INFINITY), is(nullValue()));
    }

    @Test
    @DisplayName("double result too large for a double rejects with null")
    void doubleOverflowingResult() {
        assertThat(RoundDoubleTransform.of(-308, "UP").execute(this.ctx, Double.MAX_VALUE), is(nullValue()));
    }

    @Test
    @DisplayName("double UNNECESSARY throws for a value that needs rounding")
    void doubleUnnecessaryThrows() {
        RoundDoubleTransform stage = RoundDoubleTransform.of(1, "UNNECESSARY");
        assertThrows(ArithmeticException.class, () -> stage.execute(this.ctx, 1.25));
    }

    @Test
    @DisplayName("double UNNECESSARY passes a value already at the scale")
    void doubleUnnecessaryPassesExact() {
        assertThat(RoundDoubleTransform.of(2, "UNNECESSARY").execute(this.ctx, 1.25), is(equalTo(1.25)));
    }

    @Test
    @DisplayName("double of refuses a name that is not a RoundingMode")
    void doubleRefusesUnknownMode() {
        assertThrows(IllegalArgumentException.class, () -> RoundDoubleTransform.of(2, "HALF"));
    }

    @Test
    @DisplayName("double mode() answers HALF_UP when no mode is configured")
    void doubleResolvesDefaultMode() {
        assertThat(RoundDoubleTransform.of(2, null).mode(), is(RoundingMode.HALF_UP));
    }

    @Test
    @DisplayName("double config leaves an absent mode absent")
    void doubleConfigOmitsAbsentMode() {
        assertThat(RoundDoubleTransform.of(2, null).config().has("mode"), is(false));
    }

    @Test
    @DisplayName("double config carries the mode name as given")
    void doubleConfigKeepsModeName() {
        assertThat(RoundDoubleTransform.of(2, "CEILING").config().getString("mode"), is(equalTo("CEILING")));
    }

    @Test
    @DisplayName("A scaled division rounded at scale 0 gives the whole value")
    void doubleRoundsBinaryQuotient() {
        DataPipeline<Double> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("3.35,1.4"))
            .stage(BinaryArithmeticDoubleTransform.of(
                DataTypes.STRING,
                "DIVIDE",
                List.of(RegexExtractTransform.of("^[^,]+"), ParseDoubleTransform.of()),
                List.of(RegexExtractTransform.of("[^,]+$"), ParseDoubleTransform.of(), ArithmeticDoubleTransform.of("MULTIPLY", 0.24))
            ))
            .stage(RoundDoubleTransform.of(0, null))
            .build();
        assertThat(pipeline.execute(), is(equalTo(10.0)));
    }

    @Test
    @DisplayName("float rounds 9.97f at scale 0 to 10.0f")
    void floatRoundsToWhole() {
        assertThat(RoundFloatTransform.of(0, null).execute(this.ctx, 9.97f), is(equalTo(10.0f)));
    }

    @Test
    @DisplayName("float rounds its own decimal form, so 2.45f at scale 1 HALF_EVEN is 2.4f")
    void floatRoundsFloatDecimalForm() {
        assertThat(RoundFloatTransform.of(1, "HALF_EVEN").execute(this.ctx, 2.45f), is(equalTo(2.4f)));
    }

    @Test
    @DisplayName("float with no mode rounds a half up")
    void floatDefaultsToHalfUp() {
        assertThat(RoundFloatTransform.of(1, null).execute(this.ctx, 2.45f), is(equalTo(2.5f)));
    }

    @Test
    @DisplayName("float null input rejects with null")
    void floatNullInput() {
        assertThat(RoundFloatTransform.of(2, null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("float NaN input rejects with null")
    void floatNaNInput() {
        assertThat(RoundFloatTransform.of(2, null).execute(this.ctx, Float.NaN), is(nullValue()));
    }

    @Test
    @DisplayName("float result too large for a float rejects with null")
    void floatOverflowingResult() {
        assertThat(RoundFloatTransform.of(-38, "UP").execute(this.ctx, Float.MAX_VALUE), is(nullValue()));
    }

    @Test
    @DisplayName("float of refuses a name that is not a RoundingMode")
    void floatRefusesUnknownMode() {
        assertThrows(IllegalArgumentException.class, () -> RoundFloatTransform.of(2, "half_up"));
    }

    @Test
    @DisplayName("A wire mode that names no RoundingMode fails at load")
    void wireUnknownModeFailsAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"DOUBLE\",\"value\":\"1.5\"},"
            + "{\"kind\":\"TRANSFORM_ROUND_DOUBLE\",\"scale\":0,\"mode\":\"NEAREST\"}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @DisplayName("A wire stage with no mode rounds half up")
    void wireAbsentModeRoundsHalfUp() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"DOUBLE\",\"value\":\"2.5\"},"
            + "{\"kind\":\"TRANSFORM_ROUND_DOUBLE\",\"scale\":0}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo(3.0)));
    }

    @Test
    @DisplayName("The wire form of a stage with no mode carries no mode key")
    void wireOmitsAbsentMode() {
        DataPipeline<Double> pipeline = DataPipeline.builder()
            .source(LiteralSource.doubleVal(9.97))
            .stage(RoundDoubleTransform.of(0, null))
            .build();
        assertThat(PipelineGson.toJson(pipeline), not(containsString("\"mode\"")));
    }

    @TestFactory
    @DisplayName("Each round stage round-trips through the wire to the same config and output")
    Stream<DynamicTest> wireRoundTrips() {
        return wirePipelines().stream().flatMap(pipeline -> {
            String json = PipelineGson.toJson(pipeline);
            DataPipeline<?> rebuilt = PipelineGson.fromJson(json);
            String name = pipeline.stages().getLast().summary();
            return Stream.of(
                DynamicTest.dynamicTest(name + " kind", () -> assertThat(rebuilt.stages().getLast().kindId(), is(equalTo(pipeline.stages().getLast().kindId())))),
                DynamicTest.dynamicTest(name + " config", () -> assertThat(PipelineGson.toJson(rebuilt), is(equalTo(json)))),
                DynamicTest.dynamicTest(name + " output", () -> assertThat(rebuilt.execute(), is(equalTo(pipeline.execute()))))
            );
        });
    }

}
