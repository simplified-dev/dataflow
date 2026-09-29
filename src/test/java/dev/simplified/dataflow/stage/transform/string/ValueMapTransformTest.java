package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ValueMapTransform}: the exact whole-input match, the three ways a missing key
 * resolves, the factory's refusal, the table's order and the wire round trip.
 */
class ValueMapTransformTest {

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Map<String, String> colours() {
        Map<String, String> table = new LinkedHashMap<>();
        table.put("PINK", "LIGHT_PURPLE");
        table.put("GREY", "GRAY");
        table.put("AQUA", "AQUA");
        return table;
    }

    private static @NotNull DataPipeline<String> pipeline(@NotNull String value, @NotNull ValueMapTransform stage) {
        return DataPipeline.builder()
            .source(LiteralSource.text(value))
            .stage(stage)
            .build();
    }

    @Test
    @DisplayName("A null input yields null")
    void nullInYieldsNull() {
        assertThat(ValueMapTransform.of(colours(), "WHITE", null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A key in the table returns its value")
    void keyReturnsValue() {
        assertThat(ValueMapTransform.of(colours(), null, null).execute(this.ctx, "PINK"), is(equalTo("LIGHT_PURPLE")));
    }

    @Test
    @DisplayName("A key maps to its value before the default applies")
    void keyWinsOverDefault() {
        assertThat(ValueMapTransform.of(colours(), "WHITE", null).execute(this.ctx, "GREY"), is(equalTo("GRAY")));
    }

    @Test
    @DisplayName("A key maps to its value in strict mode")
    void keyMapsWhenStrict() {
        assertThat(ValueMapTransform.of(colours(), null, true).execute(this.ctx, "GREY"), is(equalTo("GRAY")));
    }

    @Test
    @DisplayName("A key mapped to an empty string returns the empty string")
    void keyMapsToEmptyString() {
        assertThat(ValueMapTransform.of(Map.of("NONE", ""), "WHITE", null).execute(this.ctx, "NONE"), is(equalTo("")));
    }

    @Test
    @DisplayName("The match is case-sensitive")
    void matchIsCaseSensitive() {
        assertThat(ValueMapTransform.of(colours(), null, null).execute(this.ctx, "pink"), is(equalTo("pink")));
    }

    @Test
    @DisplayName("The match is on the whole input, not a part of it")
    void matchIsOnWholeInput() {
        assertThat(ValueMapTransform.of(colours(), null, null).execute(this.ctx, "PINKISH"), is(equalTo("PINKISH")));
    }

    @Test
    @DisplayName("A missing key passes through unchanged when neither default nor strict is set")
    void missingKeyPassesThrough() {
        assertThat(ValueMapTransform.of(colours(), null, null).execute(this.ctx, "BLACK"), is(equalTo("BLACK")));
    }

    @Test
    @DisplayName("A missing key passes through unchanged when strict is false")
    void missingKeyPassesThroughWhenNotStrict() {
        assertThat(ValueMapTransform.of(colours(), null, false).execute(this.ctx, "BLACK"), is(equalTo("BLACK")));
    }

    @Test
    @DisplayName("A missing key returns the default value")
    void missingKeyReturnsDefault() {
        assertThat(ValueMapTransform.of(colours(), "WHITE", null).execute(this.ctx, "BLACK"), is(equalTo("WHITE")));
    }

    @Test
    @DisplayName("A missing key returns the default value when strict is false")
    void missingKeyReturnsDefaultWhenNotStrict() {
        assertThat(ValueMapTransform.of(colours(), "WHITE", false).execute(this.ctx, "BLACK"), is(equalTo("WHITE")));
    }

    @Test
    @DisplayName("A missing key rejects with null in strict mode")
    void missingKeyRejectsWhenStrict() {
        assertThat(ValueMapTransform.of(colours(), null, true).execute(this.ctx, "BLACK"), is(nullValue()));
    }

    @Test
    @DisplayName("An empty table with a default maps every input to the default")
    void emptyTableReturnsDefault() {
        assertThat(ValueMapTransform.of(Map.of(), "WHITE", null).execute(this.ctx, "PINK"), is(equalTo("WHITE")));
    }

    @Test
    @DisplayName("of refuses a default value together with strict")
    void ofRefusesDefaultWithStrict() {
        assertThrows(IllegalArgumentException.class, () -> ValueMapTransform.of(colours(), "WHITE", true));
    }

    @Test
    @DisplayName("of refuses a table holding a null key, which the wire form cannot carry")
    void ofRefusesNullKey() {
        Map<String, String> table = colours();
        table.put(null, "WHITE");
        assertThrows(IllegalArgumentException.class, () -> ValueMapTransform.of(table, null, null));
    }

    @Test
    @DisplayName("of refuses a table mapping a key to null, which the wire form cannot carry")
    void ofRefusesNullValue() {
        Map<String, String> table = colours();
        table.put("BLACK", null);
        assertThrows(IllegalArgumentException.class, () -> ValueMapTransform.of(table, null, null));
    }

    @Test
    @DisplayName("The table keeps the caller's order")
    void tableKeepsOrder() {
        assertThat(List.copyOf(ValueMapTransform.of(colours(), null, null).table().keySet()), contains("PINK", "GREY", "AQUA"));
    }

    @Test
    @DisplayName("The table is a copy the caller's later changes do not reach")
    void tableIsCopied() {
        Map<String, String> table = colours();
        ValueMapTransform stage = ValueMapTransform.of(table, null, null);
        table.put("BLACK", "DARK_GRAY");
        assertThat(stage.execute(this.ctx, "BLACK"), is(equalTo("BLACK")));
    }

    @Test
    @DisplayName("The table is unmodifiable")
    void tableIsUnmodifiable() {
        ValueMapTransform stage = ValueMapTransform.of(colours(), null, null);
        assertThrows(UnsupportedOperationException.class, () -> stage.table().put("BLACK", "DARK_GRAY"));
    }

    @Test
    @DisplayName("A comma-joined value followed by a split yields several values")
    void commaJoinedValueSplits() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("WEAPON"))
            .stage(ValueMapTransform.of(Map.of("WEAPON", "SWORD,BOW"), null, true))
            .stage(SplitTransform.of(","))
            .build();
        assertThat(pipeline.execute(), is(equalTo(List.of("SWORD", "BOW"))));
    }

    @Test
    @DisplayName("The wire form carries the table as a JSON object in its order")
    void wireFormCarriesOrderedTable() {
        String json = PipelineGson.toJson(pipeline("PINK", ValueMapTransform.of(colours(), null, null)));
        assertThat(json, containsString("\"table\":{\"PINK\":\"LIGHT_PURPLE\",\"GREY\":\"GRAY\",\"AQUA\":\"AQUA\"}"));
    }

    @Test
    @DisplayName("The wire form omits an unset default value")
    void wireFormOmitsUnsetDefault() {
        String json = PipelineGson.toJson(pipeline("PINK", ValueMapTransform.of(colours(), null, true)));
        assertThat(json, not(containsString("defaultValue")));
    }

    @Test
    @DisplayName("The wire form omits an unset strict flag")
    void wireFormOmitsUnsetStrict() {
        String json = PipelineGson.toJson(pipeline("PINK", ValueMapTransform.of(colours(), "WHITE", null)));
        assertThat(json, not(containsString("strict")));
    }

    @Test
    @DisplayName("A pipeline with a default value round-trips to the same JSON")
    void wireRoundTripWithDefaultIsStable() {
        String first = PipelineGson.toJson(pipeline("BLACK", ValueMapTransform.of(colours(), "WHITE", null)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with a default value round-trips to the same output")
    void wireRoundTripWithDefaultExecutes() {
        DataPipeline<String> original = pipeline("BLACK", ValueMapTransform.of(colours(), "WHITE", null));
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(original)).execute(), is(equalTo(original.execute())));
    }

    @Test
    @DisplayName("A strict pipeline round-trips to the same JSON")
    void wireRoundTripStrictIsStable() {
        String first = PipelineGson.toJson(pipeline("BLACK", ValueMapTransform.of(colours(), null, true)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with strict set to false round-trips to the same JSON")
    void wireRoundTripNotStrictIsStable() {
        String first = PipelineGson.toJson(pipeline("BLACK", ValueMapTransform.of(colours(), "WHITE", false)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A strict pipeline round-trips to the same rejection")
    void wireRoundTripStrictExecutes() {
        String json = PipelineGson.toJson(pipeline("BLACK", ValueMapTransform.of(colours(), null, true)));
        assertThat(PipelineGson.fromJson(json).execute(), is(nullValue()));
    }

    @Test
    @DisplayName("A rebuilt stage keeps the table's order")
    void wireRoundTripKeepsOrder() {
        String json = PipelineGson.toJson(pipeline("PINK", ValueMapTransform.of(colours(), null, null)));
        ValueMapTransform rebuilt = (ValueMapTransform) PipelineGson.fromJson(json).stages().getLast();
        assertThat(List.copyOf(rebuilt.table().keySet()), contains("PINK", "GREY", "AQUA"));
    }

    @Test
    @DisplayName("A wire file naming a default value and strict fails to load")
    void wireRefusesDefaultWithStrict() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"x\"},"
            + "{\"kind\":\"TRANSFORM_VALUE_MAP\",\"table\":{\"a\":\"b\"},\"defaultValue\":\"c\",\"strict\":true}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("ValueMapTransform takes 'defaultValue' or 'strict'"));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

}
