package dev.simplified.dataflow.stage.transform.primitive;

import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.SplitTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConstantTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    private <T> T constant(@NotNull DataType<T> outputType, @NotNull String value) {
        return ConstantTransform.of(DataTypes.STRING, outputType, value).execute(this.ctx, "input");
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull DataPipeline<List<Integer>> marked() {
        return DataPipeline.builder()
            .source(LiteralSource.text("a1,b,c3"))
            .stage(SplitTransform.of(","))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.INT, List.of(
                RegexExtractTransform.of("\\d"),
                ConstantTransform.of(DataTypes.STRING, DataTypes.INT, "1")
            )))
            .build();
    }

    @Test
    @DisplayName("A null input stays null")
    void nullInNullOut() {
        assertThat(ConstantTransform.of(DataTypes.STRING, DataTypes.INT, "3").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A STRING constant is emitted exactly as configured, untrimmed")
    void stringVerbatim() {
        assertThat(constant(DataTypes.STRING, " a b "), is(equalTo(" a b ")));
    }

    @Test
    @DisplayName("A RAW_HTML constant is emitted exactly as configured")
    void rawHtmlVerbatim() {
        assertThat(constant(DataTypes.RAW_HTML, "<p>a</p>"), is(equalTo("<p>a</p>")));
    }

    @Test
    @DisplayName("An INT constant is parsed after trimming")
    void intParsed() {
        assertThat(constant(DataTypes.INT, " 3 "), is(equalTo(3)));
    }

    @Test
    @DisplayName("A LONG constant is parsed")
    void longParsed() {
        assertThat(constant(DataTypes.LONG, "9000000000"), is(equalTo(9_000_000_000L)));
    }

    @Test
    @DisplayName("A FLOAT constant is parsed")
    void floatParsed() {
        assertThat(constant(DataTypes.FLOAT, "1.5"), is(equalTo(1.5f)));
    }

    @Test
    @DisplayName("A DOUBLE constant is parsed")
    void doubleParsed() {
        assertThat(constant(DataTypes.DOUBLE, "2.25"), is(equalTo(2.25)));
    }

    @Test
    @DisplayName("A BOOLEAN constant reads true in any case")
    void booleanTrue() {
        assertThat(constant(DataTypes.BOOLEAN, "TRUE"), is(true));
    }

    @Test
    @DisplayName("A BOOLEAN constant reads false")
    void booleanFalse() {
        assertThat(constant(DataTypes.BOOLEAN, " false "), is(false));
    }

    @Test
    @DisplayName("of refuses a BOOLEAN value other than true or false")
    void booleanRefusesOtherWords() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.BOOLEAN, "yes"));
    }

    @Test
    @DisplayName("of refuses an INT value that does not parse")
    void intRefusesGarbage() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.INT, "abc"));
    }

    @Test
    @DisplayName("of refuses a fractional INT value")
    void intRefusesFraction() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.INT, "3.5"));
    }

    @Test
    @DisplayName("of refuses a FLOAT value that overflows to infinity")
    void floatRefusesOverflow() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.FLOAT, "1e40"));
    }

    @Test
    @DisplayName("of refuses NaN as a DOUBLE value")
    void doubleRefusesNaN() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.DOUBLE, "NaN"));
    }

    @Test
    @DisplayName("of refuses an infinite DOUBLE value")
    void doubleRefusesInfinity() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.DOUBLE, "-Infinity"));
    }

    @Test
    @DisplayName("of refuses an output type SOURCE_LITERAL does not admit")
    void refusesStructuredType() {
        assertThrows(IllegalArgumentException.class, () -> ConstantTransform.of(DataTypes.STRING, DataTypes.JSON_OBJECT, "{}"));
    }

    @Test
    @DisplayName("Any input type is accepted and replaced")
    void anyInputType() {
        ConstantTransform<JsonObject, String> stage = ConstantTransform.of(DataTypes.JSON_OBJECT, DataTypes.STRING, "row");
        assertThat(stage.execute(this.ctx, new JsonObject()), is(equalTo("row")));
    }

    @Test
    @DisplayName("Every input yields the one value parsed when the stage was built")
    void parsedOnce() {
        ConstantTransform<String, Long> stage = ConstantTransform.of(DataTypes.STRING, DataTypes.LONG, "9000000000");
        assertThat(stage.execute(this.ctx, "a"), is(sameInstance(stage.execute(this.ctx, "b"))));
    }

    @Test
    @DisplayName("The stage advertises its configured input type")
    void advertisesInputType() {
        assertThat(ConstantTransform.of(DataTypes.JSON_OBJECT, DataTypes.INT, "1").inputType(), is(sameInstance(DataTypes.JSON_OBJECT)));
    }

    @Test
    @DisplayName("The stage advertises its configured output type")
    void advertisesOutputType() {
        assertThat(ConstantTransform.of(DataTypes.JSON_OBJECT, DataTypes.INT, "1").outputType(), is(sameInstance(DataTypes.INT)));
    }

    @Test
    @DisplayName("config() carries the value as configured, not the parsed constant")
    void configCarriesRawValue() {
        assertThat(ConstantTransform.of(DataTypes.STRING, DataTypes.INT, " 3 ").config().getString("value"), is(equalTo(" 3 ")));
    }

    @Test
    @DisplayName("Inside a map body the constant marks present elements only")
    void marksPresentElementsInBody() {
        assertThat(marked().execute(this.ctx), contains(1, 1));
    }

    @Test
    @DisplayName("A pipeline with a non-string constant round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(marked());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with a non-string constant round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(marked()));
        assertThat(rebuilt.execute(this.ctx), is(equalTo(marked().execute(this.ctx))));
    }

    @Test
    @DisplayName("The wire form carries the value as its configured string")
    void wireFormCarriesString() {
        DataPipeline<Integer> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(ConstantTransform.of(DataTypes.STRING, DataTypes.INT, "3"))
            .build();
        assertThat(
            PipelineGson.toJson(pipeline),
            containsString("{\"kind\":\"TRANSFORM_CONSTANT\",\"inputType\":\"STRING\",\"outputType\":\"INT\",\"value\":\"3\"}")
        );
    }

    @Test
    @DisplayName("A value that does not parse fails the load")
    void wireRejectsUnparsableValue() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TRANSFORM_CONSTANT\",\"inputType\":\"STRING\",\"outputType\":\"INT\",\"value\":\"x\"}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

}
