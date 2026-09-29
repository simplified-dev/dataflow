package dev.simplified.dataflow.stage.source;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.serde.PipelineGson;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers the configuration and wire form of a {@link LiteralSource} whose output type is not a
 * string. Before, {@code config()} read the parsed value into the {@code STRING} slot and both it
 * and {@code PipelineGson.toJson} threw {@link ClassCastException}.
 */
class LiteralSourceSerdeTest {

    private static @NotNull String json(@NotNull LiteralSource<?> source) {
        return PipelineGson.toJson(DataPipeline.builder().source(source).build());
    }

    @Test
    @DisplayName("config() of an INT literal carries the configured string")
    void intConfigCarriesString() {
        assertThat(LiteralSource.of(DataTypes.INT, " 3 ").config().getString("value"), is(equalTo(" 3 ")));
    }

    @Test
    @DisplayName("An INT literal serialises its value as the configured string")
    void intSerialises() {
        assertThat(json(LiteralSource.integerVal(3)), is(equalTo("[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"3\"}]")));
    }

    @Test
    @DisplayName("An INT literal round-trips to the same value")
    void intRoundTrips() {
        assertThat(PipelineGson.fromJson(json(LiteralSource.integerVal(3))).execute(), is(equalTo(3)));
    }

    @Test
    @DisplayName("A LONG literal round-trips to the same value")
    void longRoundTrips() {
        assertThat(PipelineGson.fromJson(json(LiteralSource.longVal(9_000_000_000L))).execute(), is(equalTo(9_000_000_000L)));
    }

    @Test
    @DisplayName("A FLOAT literal round-trips to the same value")
    void floatRoundTrips() {
        assertThat(PipelineGson.fromJson(json(LiteralSource.floatVal(1.5f))).execute(), is(equalTo(1.5f)));
    }

    @Test
    @DisplayName("A DOUBLE literal round-trips to the same value")
    void doubleRoundTrips() {
        assertThat(PipelineGson.fromJson(json(LiteralSource.doubleVal(2.25))).execute(), is(equalTo(2.25)));
    }

    @Test
    @DisplayName("A BOOLEAN literal round-trips to the same value")
    void booleanRoundTrips() {
        assertThat(PipelineGson.fromJson(json(LiteralSource.booleanVal(true))).execute(), is(equalTo(true)));
    }

    @Test
    @DisplayName("A non-string literal's JSON is stable across a round trip")
    void jsonIsStable() {
        String first = json(LiteralSource.doubleVal(2.25));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The parsed value stays readable through value()")
    void parsedValueAccessor() {
        assertThat(LiteralSource.integerVal(3).value(), is(equalTo(3)));
    }

}
