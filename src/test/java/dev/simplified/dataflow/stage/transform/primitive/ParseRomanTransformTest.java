package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import dev.simplified.dataflow.stage.transform.string.SplitTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ParseRomanTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    private @Nullable Integer parse(@Nullable String input) {
        return ParseRomanTransform.of().execute(this.ctx, input);
    }

    private static @NotNull DataPipeline<List<Integer>> levels() {
        return DataPipeline.builder()
            .source(LiteralSource.text("I,iv,XV,bad,MMMCMXCIX"))
            .stage(SplitTransform.of(","))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.INT, List.of(ParseRomanTransform.of())))
            .build();
    }

    @Test
    @DisplayName("A null input stays null")
    void nullInNullOut() {
        assertThat(parse(null), is(nullValue()));
    }

    @TestFactory
    @DisplayName("Canonical numerals read as their value")
    Stream<DynamicTest> canonicalNumerals() {
        Map<String, Integer> cases = Map.ofEntries(
            Map.entry("I", 1),
            Map.entry("III", 3),
            Map.entry("IV", 4),
            Map.entry("V", 5),
            Map.entry("IX", 9),
            Map.entry("XIV", 14),
            Map.entry("XV", 15),
            Map.entry("XL", 40),
            Map.entry("XC", 90),
            Map.entry("CD", 400),
            Map.entry("CM", 900),
            Map.entry("MCMXCIV", 1994),
            Map.entry("MMXXVI", 2026),
            Map.entry("MMMCMXCIX", 3999)
        );
        return cases.entrySet().stream().map(entry -> DynamicTest.dynamicTest(
            entry.getKey(),
            () -> assertThat(parse(entry.getKey()), is(equalTo(entry.getValue())))
        ));
    }

    @Test
    @DisplayName("Every value from 1 to 3999 reads back from its canonical numeral")
    void everyValueRoundTrips() {
        List<Integer> mismatches = new ArrayList<>();

        for (int value = 1; value <= 3999; value++)
            if (!Integer.valueOf(value).equals(parse(roman(value)))) mismatches.add(value);

        assertThat(mismatches, is(empty()));
    }

    @Test
    @DisplayName("Lower case reads as upper case")
    void lowerCase() {
        assertThat(parse("xiv"), is(equalTo(14)));
    }

    @Test
    @DisplayName("Mixed case reads as upper case")
    void mixedCase() {
        assertThat(parse("mCmXcIv"), is(equalTo(1994)));
    }

    @Test
    @DisplayName("Surrounding whitespace is trimmed")
    void trimmed() {
        assertThat(parse("  XV\t"), is(equalTo(15)));
    }

    @TestFactory
    @DisplayName("A string that is not a canonical numeral yields null")
    Stream<DynamicTest> nonCanonicalYieldsNull() {
        String unicodeNumeral = new String(new char[] { 0x2169, 0x2164 });
        String dotlessI = new String(new char[] { 0x0131, 'v' });
        return Stream.of("", "   ", "IIII", "VX", "IC", "IL", "XM", "VV", "LL", "DD", "MMMM", "IIV", "XIIII", "Combat XV", "X V", "15", unicodeNumeral, dotlessI)
            .map(input -> DynamicTest.dynamicTest(
                "'" + input + "'",
                () -> assertThat(parse(input), is(nullValue()))
            ));
    }

    @Test
    @DisplayName("The stage reads STRING and produces INT")
    void types() {
        ParseRomanTransform stage = ParseRomanTransform.of();
        assertThat(List.of(stage.inputType(), stage.outputType()), contains(DataTypes.STRING, DataTypes.INT));
    }

    @Test
    @DisplayName("In a map body a string that is not a numeral drops out")
    void dropsInBody() {
        assertThat(levels().execute(this.ctx), contains(1, 4, 15, 3999));
    }

    @Test
    @DisplayName("The wire form carries no configuration")
    void wireForm() {
        assertThat(PipelineGson.toJson(levels()), containsString("[{\"kind\":\"TRANSFORM_PARSE_ROMAN\"}]"));
    }

    @Test
    @DisplayName("A pipeline with the stage round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(levels());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with the stage round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(levels()));
        assertThat(rebuilt.execute(this.ctx), is(equalTo(levels().execute(this.ctx))));
    }

    /**
     * Writes {@code value} as its canonical numeral, independently of the stage under test.
     */
    private static @NotNull String roman(int value) {
        int[] values = { 1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1 };
        String[] symbols = { "M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I" };
        StringBuilder out = new StringBuilder();
        int rest = value;

        for (int i = 0; i < values.length; i++) {
            while (rest >= values[i]) {
                out.append(symbols[i]);
                rest -= values[i];
            }
        }

        return out.toString();
    }

}
