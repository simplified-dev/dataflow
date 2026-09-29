package dev.simplified.dataflow.stage.predicate.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.filter.list.WhereFilter;
import dev.simplified.dataflow.stage.predicate.string.EndsWithPredicate;
import dev.simplified.dataflow.stage.predicate.string.StartsWithPredicate;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.AsIntTransform;
import dev.simplified.dataflow.stage.transform.json.PathTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseLongTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ComparePredicate} and {@link CompareOperator}. Most cases read a
 * {@code "left,right"} string, the left body taking the text before the comma and the right body
 * the text after it.
 */
class ComparePredicateTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"3,10\"}";

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull List<Stage<?, ?>> before(@NotNull Stage<?, ?> parse) {
        return List.of(RegexExtractTransform.of("^[^,]+"), parse);
    }

    private static @NotNull List<Stage<?, ?>> after(@NotNull Stage<?, ?> parse) {
        return List.of(RegexExtractTransform.of("[^,]+$"), parse);
    }

    private static @NotNull List<Stage<?, ?>> before() {
        return List.of(RegexExtractTransform.of("^[^,]+"));
    }

    private static @NotNull List<Stage<?, ?>> after() {
        return List.of(RegexExtractTransform.of("[^,]+$"));
    }

    private static @NotNull ComparePredicate<String, Integer> ints(@NotNull String operator) {
        return ComparePredicate.of(DataTypes.STRING, DataTypes.INT, operator, before(ParseIntTransform.of()), after(ParseIntTransform.of()));
    }

    private static @NotNull ComparePredicate<String, Long> longs(@NotNull String operator) {
        return ComparePredicate.of(DataTypes.STRING, DataTypes.LONG, operator, before(ParseLongTransform.of()), after(ParseLongTransform.of()));
    }

    private static @NotNull ComparePredicate<String, Float> floats(@NotNull String operator) {
        return ComparePredicate.of(DataTypes.STRING, DataTypes.FLOAT, operator, before(ParseFloatTransform.of()), after(ParseFloatTransform.of()));
    }

    private static @NotNull ComparePredicate<String, Double> doubles(@NotNull String operator) {
        return ComparePredicate.of(DataTypes.STRING, DataTypes.DOUBLE, operator, before(ParseDoubleTransform.of()), after(ParseDoubleTransform.of()));
    }

    private static @NotNull ComparePredicate<String, String> strings(@NotNull String operator) {
        return ComparePredicate.of(DataTypes.STRING, DataTypes.STRING, operator, before(), after());
    }

    private static @NotNull ComparePredicate<String, Boolean> booleans(@NotNull String operator) {
        return ComparePredicate.of(
            DataTypes.STRING, DataTypes.BOOLEAN, operator, List.of(StartsWithPredicate.of("a")), List.of(EndsWithPredicate.of("a"))
        );
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<DataPipeline<?>> wirePipelines() {
        return List.of(
            DataPipeline.builder().source(LiteralSource.text("3,10")).stage(ints("LESS_THAN")).build(),
            DataPipeline.builder().source(LiteralSource.text("3000000000,7")).stage(longs("GREATER_THAN")).build(),
            DataPipeline.builder().source(LiteralSource.text("1.5,1.5")).stage(floats("LESS_OR_EQUAL")).build(),
            DataPipeline.builder().source(LiteralSource.text("2.5,1")).stage(doubles("GREATER_OR_EQUAL")).build(),
            DataPipeline.builder().source(LiteralSource.text("pear,plum")).stage(strings("NOT_EQUALS")).build(),
            DataPipeline.builder().source(LiteralSource.text("abca")).stage(booleans("EQUALS")).build()
        );
    }

    @TestFactory
    @DisplayName("Each operator tests left against right for less, equal and greater INT values")
    Stream<DynamicTest> operatorTable() {
        Map<String, List<Boolean>> expected = Map.of(
            "EQUALS", List.of(false, true, false),
            "NOT_EQUALS", List.of(true, false, true),
            "LESS_THAN", List.of(true, false, false),
            "LESS_OR_EQUAL", List.of(true, true, false),
            "GREATER_THAN", List.of(false, false, true),
            "GREATER_OR_EQUAL", List.of(false, true, true)
        );
        List<String> inputs = List.of("3,10", "10,10", "10,3");
        return expected.entrySet().stream().flatMap(entry -> Stream.of(0, 1, 2).map(i -> DynamicTest.dynamicTest(
            entry.getKey() + " " + inputs.get(i),
            () -> assertThat(ints(entry.getKey()).execute(this.ctx, inputs.get(i)), is(equalTo(entry.getValue().get(i))))
        )));
    }

    @Test
    @DisplayName("INT values compare numerically, not by their text")
    void intComparesNumerically() {
        assertThat(ints("LESS_THAN").execute(this.ctx, "9,10"), is(true));
    }

    @Test
    @DisplayName("LONG values compare past the int range")
    void longComparesPastIntRange() {
        assertThat(longs("GREATER_THAN").execute(this.ctx, "3000000000,2999999999"), is(true));
    }

    @Test
    @DisplayName("FLOAT negative zero equals zero")
    void floatNegativeZeroEqualsZero() {
        assertThat(floats("EQUALS").execute(this.ctx, "-0.0,0.0"), is(true));
    }

    @Test
    @DisplayName("FLOAT values compare by fraction")
    void floatComparesFraction() {
        assertThat(floats("LESS_THAN").execute(this.ctx, "1.25,1.5"), is(true));
    }

    @Test
    @DisplayName("DOUBLE values compare by fraction")
    void doubleComparesFraction() {
        assertThat(doubles("GREATER_OR_EQUAL").execute(this.ctx, "2.5,2.49"), is(true));
    }

    @Test
    @DisplayName("DOUBLE NaN on the left yields null")
    void doubleLeftNaNYieldsNull() {
        assertThat(doubles("NOT_EQUALS").execute(this.ctx, "NaN,1"), is(nullValue()));
    }

    @Test
    @DisplayName("FLOAT NaN on the right yields null")
    void floatRightNaNYieldsNull() {
        assertThat(floats("LESS_THAN").execute(this.ctx, "1,NaN"), is(nullValue()));
    }

    @Test
    @DisplayName("STRING values compare by text, so 10 sorts before 9")
    void stringComparesText() {
        assertThat(strings("LESS_THAN").execute(this.ctx, "10,9"), is(true));
    }

    @Test
    @DisplayName("STRING comparison is case-sensitive, upper case before lower")
    void stringComparisonIsCaseSensitive() {
        assertThat(strings("LESS_THAN").execute(this.ctx, "Zebra,apple"), is(true));
    }

    @Test
    @DisplayName("STRING EQUALS matches equal text")
    void stringEquals() {
        assertThat(strings("EQUALS").execute(this.ctx, "pear,pear"), is(true));
    }

    @Test
    @DisplayName("BOOLEAN EQUALS is true when both bodies agree")
    void booleanEqualsAgree() {
        assertThat(booleans("EQUALS").execute(this.ctx, "abca"), is(true));
    }

    @Test
    @DisplayName("BOOLEAN EQUALS is false when the bodies disagree")
    void booleanEqualsDisagree() {
        assertThat(booleans("EQUALS").execute(this.ctx, "abc"), is(false));
    }

    @Test
    @DisplayName("BOOLEAN NOT_EQUALS is true when the bodies disagree")
    void booleanNotEqualsDisagree() {
        assertThat(booleans("NOT_EQUALS").execute(this.ctx, "abc"), is(true));
    }

    @Test
    @DisplayName("A null input yields null")
    void nullInputYieldsNull() {
        assertThat(ints("EQUALS").execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A left body yielding null yields null")
    void leftNullYieldsNull() {
        assertThat(ints("NOT_EQUALS").execute(this.ctx, "x,2"), is(nullValue()));
    }

    @Test
    @DisplayName("A right body yielding null yields null")
    void rightNullYieldsNull() {
        assertThat(ints("NOT_EQUALS").execute(this.ctx, "7,x"), is(nullValue()));
    }

    @Test
    @DisplayName("The right body does not run when the left body yields null")
    void rightSkippedAfterLeftNull() {
        ParseIntTransform watched = ParseIntTransform.of();
        ComparePredicate<String, Integer> stage = ComparePredicate.of(
            DataTypes.STRING, DataTypes.INT, "EQUALS", before(ParseIntTransform.of()), after(watched)
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
    @DisplayName("Bodies read two fields of one JSON row")
    void comparesTwoFieldsOfRow() {
        ComparePredicate<JsonElement, Integer> stage = ComparePredicate.of(
            DataTypes.JSON_ELEMENT,
            DataTypes.INT,
            "GREATER_THAN",
            List.of(PathTransform.of("a"), AsIntTransform.of()),
            List.of(PathTransform.of("b"), AsIntTransform.of())
        );
        assertThat(stage.execute(this.ctx, JsonParser.parseString("{\"a\":9,\"b\":4}")), is(true));
    }

    @Test
    @DisplayName("FILTER_WHERE over a comparison keeps passing rows and drops a row missing a side")
    void whereDropsUndecidedRows() {
        WhereFilter<JsonElement> where = WhereFilter.of(DataTypes.JSON_ELEMENT, List.of(ComparePredicate.of(
            DataTypes.JSON_ELEMENT,
            DataTypes.INT,
            "LESS_THAN",
            List.of(PathTransform.of("a"), AsIntTransform.of()),
            List.of(PathTransform.of("b"), AsIntTransform.of())
        )));
        List<JsonElement> rows = List.of(
            JsonParser.parseString("{\"id\":1,\"a\":1,\"b\":2}"),
            JsonParser.parseString("{\"id\":2,\"a\":3,\"b\":2}"),
            JsonParser.parseString("{\"id\":3,\"a\":0}"),
            JsonParser.parseString("{\"id\":4,\"a\":0,\"b\":5}")
        );
        List<JsonElement> kept = where.execute(this.ctx, rows);
        assertThat(kept, is(equalTo(List.of(rows.get(0), rows.get(3)))));
    }

    @Test
    @DisplayName("The output type is BOOLEAN")
    void outputTypeIsBoolean() {
        assertThat(ints("EQUALS").outputType(), is(equalTo(DataTypes.BOOLEAN)));
    }

    @Test
    @DisplayName("config carries the operator name as given")
    void configKeepsOperatorName() {
        assertThat(ints("GREATER_OR_EQUAL").config().getString("operator"), is(equalTo("GREATER_OR_EQUAL")));
    }

    @Test
    @DisplayName("isOrdering is false for EQUALS and NOT_EQUALS")
    void equalityOperatorsDoNotOrder() {
        assertThat(CompareOperator.EQUALS.isOrdering() || CompareOperator.NOT_EQUALS.isOrdering(), is(false));
    }

    @Test
    @DisplayName("CompareOperator.of refuses a lower-case name")
    void operatorNameIsExact() {
        assertThrows(IllegalArgumentException.class, () -> CompareOperator.of("equals"));
    }

    @Test
    @DisplayName("of refuses an unknown operator")
    void refusesUnknownOperator() {
        assertThrows(IllegalArgumentException.class, () -> ints("BETWEEN"));
    }

    @Test
    @DisplayName("of refuses a value type outside the supported six")
    void refusesUnsupportedValueType() {
        DataType<JsonElement> unsupported = DataTypes.JSON_ELEMENT;
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ComparePredicate.of(
            DataTypes.JSON_ELEMENT, unsupported, "EQUALS", List.of(PathTransform.of("a")), List.of(PathTransform.of("b"))
        ));
        assertThat(thrown.getMessage(), startsWith("ComparePredicate supports value types"));
    }

    @Test
    @DisplayName("of refuses an ordering operator over BOOLEAN values")
    void refusesOrderedBooleans() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> booleans("LESS_THAN"));
        assertThat(thrown.getMessage(), startsWith("ComparePredicate compares BOOLEAN values only with EQUALS or NOT_EQUALS"));
    }

    @Test
    @DisplayName("of refuses a left body that does not produce the value type")
    void refusesMistypedLeft() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ComparePredicate.of(
            DataTypes.STRING, DataTypes.INT, "EQUALS", before(), after(ParseIntTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ComparePredicate body 'left'"));
    }

    @Test
    @DisplayName("of refuses a right body whose type chain breaks")
    void refusesBrokenRight() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ComparePredicate.of(
            DataTypes.STRING, DataTypes.INT, "EQUALS", before(ParseIntTransform.of()), List.of(LengthTransform.of(), ParseIntTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ComparePredicate body 'right'"));
    }

    @Test
    @DisplayName("of refuses an empty body")
    void refusesEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> ComparePredicate.of(
            DataTypes.STRING, DataTypes.STRING, "EQUALS", before(), List.of()
        ));
    }

    @Test
    @DisplayName("A wire stage reads its inputType, valueType, operator, left and right keys")
    void wireReadsKeys() {
        String json = "[" + SOURCE + ",{\"kind\":\"PREDICATE_COMPARE\",\"inputType\":\"STRING\",\"valueType\":\"INT\","
            + "\"operator\":\"LESS_THAN\",\"left\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}],"
            + "\"right\":[{\"kind\":\"TRANSFORM_REGEX_EXTRACT\",\"regex\":\"\\\\d+$\",\"group\":0},{\"kind\":\"TRANSFORM_PARSE_INT\"}]}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(true));
    }

    @Test
    @DisplayName("A wire body that does not produce the value type fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"PREDICATE_COMPARE\",\"inputType\":\"STRING\",\"valueType\":\"INT\","
            + "\"operator\":\"EQUALS\",\"left\":[{\"kind\":\"TRANSFORM_TRIM\"}],\"right\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid ComparePredicate body 'left'"));
    }

    @Test
    @DisplayName("A wire ordering operator over BOOLEAN values fails at load")
    void wireOrderedBooleansFailAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"PREDICATE_COMPARE\",\"inputType\":\"STRING\",\"valueType\":\"BOOLEAN\","
            + "\"operator\":\"GREATER_THAN\",\"left\":[{\"kind\":\"PREDICATE_STRING_NON_EMPTY\"}],"
            + "\"right\":[{\"kind\":\"PREDICATE_STRING_NON_EMPTY\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("ComparePredicate compares BOOLEAN values only"));
    }

    @TestFactory
    @DisplayName("Each value type round-trips through the wire to the same config and output")
    Stream<DynamicTest> wireRoundTrips() {
        return wirePipelines().stream().flatMap(pipeline -> {
            String json = PipelineGson.toJson(pipeline);
            DataPipeline<?> rebuilt = PipelineGson.fromJson(json);
            String label = ((ComparePredicate<?, ?>) pipeline.stages().getLast()).valueType().label();
            return Stream.of(
                DynamicTest.dynamicTest(label + " kind", () -> assertThat(rebuilt.stages().getLast().kindId(), is(equalTo("PREDICATE_COMPARE")))),
                DynamicTest.dynamicTest(label + " config", () -> assertThat(PipelineGson.toJson(rebuilt), is(equalTo(json)))),
                DynamicTest.dynamicTest(label + " output", () -> assertThat(rebuilt.execute(), is(equalTo(pipeline.execute()))))
            );
        });
    }

    @Test
    @DisplayName("Every wire round-trip pipeline yields true")
    void wirePipelinesYieldTrue() {
        assertThat(wirePipelines().stream().allMatch(pipeline -> Boolean.TRUE.equals(pipeline.execute())), is(true));
    }

}
