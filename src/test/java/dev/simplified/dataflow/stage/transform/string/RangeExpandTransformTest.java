package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link RangeExpandTransform}: the default and a custom pattern, the step, every
 * rejection, the size guard, the factory's refusals and the wire round trip.
 */
class RangeExpandTransformTest {

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private @Nullable List<Integer> expand(@NotNull String input) {
        return RangeExpandTransform.of(null, null, null).execute(this.ctx, input);
    }

    private static @NotNull List<Integer> between(int low, int high) {
        return IntStream.rangeClosed(low, high).boxed().toList();
    }

    private static @NotNull DataPipeline<List<Integer>> pipeline(@NotNull String value, @NotNull RangeExpandTransform stage) {
        return DataPipeline.builder()
            .source(LiteralSource.text(value))
            .stage(stage)
            .build();
    }

    @Test
    @DisplayName("A null input yields null")
    void nullInYieldsNull() {
        assertThat(RangeExpandTransform.of(null, null, null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("The default pattern expands both bounds inclusive, ascending")
    void expandsInclusive() {
        assertThat(expand("1-15"), is(equalTo(between(1, 15))));
    }

    @Test
    @DisplayName("Equal bounds yield a single member")
    void equalBoundsYieldOneMember() {
        assertThat(expand("5-5"), is(equalTo(List.of(5))));
    }

    @Test
    @DisplayName("The default pattern reads negative bounds")
    void readsNegativeBounds() {
        assertThat(expand("-3--1"), is(equalTo(List.of(-3, -2, -1))));
    }

    @Test
    @DisplayName("The default pattern reads a range across zero")
    void readsRangeAcrossZero() {
        assertThat(expand("-2-2"), is(equalTo(List.of(-2, -1, 0, 1, 2))));
    }

    @Test
    @DisplayName("A string the pattern does not match yields null")
    void unmatchedYieldsNull() {
        assertThat(expand("skeleton"), is(nullValue()));
    }

    @Test
    @DisplayName("A single number does not match the default pattern")
    void singleNumberYieldsNull() {
        assertThat(expand("15"), is(nullValue()));
    }

    @Test
    @DisplayName("The default pattern is anchored at both ends")
    void defaultPatternIsAnchored() {
        assertThat(expand("levels 1-15"), is(nullValue()));
    }

    @Test
    @DisplayName("A low bound above the high bound yields null")
    void descendingYieldsNull() {
        assertThat(expand("15-1"), is(nullValue()));
    }

    @Test
    @DisplayName("A bound past the int range yields null")
    void boundPastIntRangeYieldsNull() {
        assertThat(expand("1-99999999999"), is(nullValue()));
    }

    @Test
    @DisplayName("A range at the top of the int range expands without overflowing")
    void rangeAtIntMaxExpands() {
        assertThat(expand("2147483646-2147483647"), is(equalTo(List.of(Integer.MAX_VALUE - 1, Integer.MAX_VALUE))));
    }

    @Test
    @DisplayName("A range at the bottom of the int range expands without overflowing")
    void rangeAtIntMinExpands() {
        assertThat(expand("-2147483648--2147483647"), is(equalTo(List.of(Integer.MIN_VALUE, Integer.MIN_VALUE + 1))));
    }

    @Test
    @DisplayName("The whole int range is refused by the size guard rather than overflowing")
    void wholeIntRangeYieldsNull() {
        assertThat(expand("-2147483648-2147483647"), is(nullValue()));
    }

    @Test
    @DisplayName("A range of exactly the default max size expands")
    void defaultMaxSizeExpands() {
        assertThat(expand("1-1000"), hasSize(1000));
    }

    @Test
    @DisplayName("A range one past the default max size yields null")
    void pastDefaultMaxSizeYieldsNull() {
        assertThat(expand("1-1001"), is(nullValue()));
    }

    @Test
    @DisplayName("A configured max size refuses a longer range")
    void configuredMaxSizeRefuses() {
        assertThat(RangeExpandTransform.of(null, null, 3).execute(this.ctx, "1-4"), is(nullValue()));
    }

    @Test
    @DisplayName("A configured max size counts members, not the span, when stepping")
    void maxSizeCountsSteppedMembers() {
        assertThat(RangeExpandTransform.of(null, 2, 3).execute(this.ctx, "1-6"), is(equalTo(List.of(1, 3, 5))));
    }

    @Test
    @DisplayName("A step skips members and stops at the last one not past the high bound")
    void stepSkipsMembers() {
        assertThat(RangeExpandTransform.of(null, 2, null).execute(this.ctx, "1-6"), is(equalTo(List.of(1, 3, 5))));
    }

    @Test
    @DisplayName("A step that lands on the high bound includes it")
    void stepLandingOnHighIncludesIt() {
        assertThat(RangeExpandTransform.of(null, 5, null).execute(this.ctx, "0-10"), is(equalTo(List.of(0, 5, 10))));
    }

    @Test
    @DisplayName("A custom pattern is searched for inside the input")
    void customPatternIsSearched() {
        RangeExpandTransform stage = RangeExpandTransform.of("(\\d+) to (\\d+)", null, null);
        assertThat(stage.execute(this.ctx, "Levels 3 to 5 only"), is(equalTo(List.of(3, 4, 5))));
    }

    @Test
    @DisplayName("A bound group that did not participate yields null")
    void nonParticipatingBoundYieldsNull() {
        RangeExpandTransform stage = RangeExpandTransform.of("^(\\d+)(?:-(\\d+))?$", null, null);
        assertThat(stage.execute(this.ctx, "5"), is(nullValue()));
    }

    @Test
    @DisplayName("A bound that is not an int yields null")
    void nonIntegerBoundYieldsNull() {
        RangeExpandTransform stage = RangeExpandTransform.of("^(\\w+)-(\\w+)$", null, null);
        assertThat(stage.execute(this.ctx, "a-b"), is(nullValue()));
    }

    @Test
    @DisplayName("The list is unmodifiable")
    void listIsUnmodifiable() {
        List<Integer> members = expand("1-3");
        assertThrows(UnsupportedOperationException.class, () -> members.add(4));
    }

    @Test
    @DisplayName("The output type is List<INT>")
    void outputTypeIsIntList() {
        assertThat(RangeExpandTransform.of(null, null, null).outputType(), is(equalTo(DataType.list(DataTypes.INT))));
    }

    @Test
    @DisplayName("of refuses a pattern with fewer than two groups")
    void ofRefusesOneGroup() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of("^(\\d+)$", null, null));
    }

    @Test
    @DisplayName("of refuses a pattern that does not compile")
    void ofRefusesBadPattern() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of("(\\d+)-(\\d+", null, null));
    }

    @Test
    @DisplayName("of refuses a step of zero")
    void ofRefusesZeroStep() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of(null, 0, null));
    }

    @Test
    @DisplayName("of refuses a negative step")
    void ofRefusesNegativeStep() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of(null, -1, null));
    }

    @Test
    @DisplayName("of refuses a max size of zero")
    void ofRefusesZeroMaxSize() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of(null, null, 0));
    }

    @Test
    @DisplayName("of accepts a max size at the ceiling")
    void ofAcceptsMaxSizeAtCeiling() {
        assertThat(RangeExpandTransform.of(null, null, 100_000).maxSize(), is(equalTo(100_000)));
    }

    @Test
    @DisplayName("of refuses a max size above the ceiling")
    void ofRefusesMaxSizeAboveCeiling() {
        assertThrows(IllegalArgumentException.class, () -> RangeExpandTransform.of(null, null, 100_001));
    }

    @Test
    @DisplayName("A wire file with a max size above the ceiling fails to load")
    void wireRefusesMaxSizeAboveCeiling() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"1-3\"},"
            + "{\"kind\":\"TRANSFORM_RANGE_EXPAND\",\"maxSize\":2147483647}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("RangeExpandTransform maxSize '2147483647' is above"));
    }

    @Test
    @DisplayName("The wire form omits every unset optional slot")
    void wireFormOmitsUnsetSlots() {
        String json = PipelineGson.toJson(pipeline("1-3", RangeExpandTransform.of(null, null, null)));
        assertThat(json, endsWith("{\"kind\":\"TRANSFORM_RANGE_EXPAND\"}]"));
    }

    @Test
    @DisplayName("A pipeline with the defaults round-trips to the same JSON")
    void wireRoundTripDefaultsIsStable() {
        String first = PipelineGson.toJson(pipeline("1-3", RangeExpandTransform.of(null, null, null)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with every slot set round-trips to the same JSON")
    void wireRoundTripConfiguredIsStable() {
        String first = PipelineGson.toJson(pipeline("Levels 0 to 10", RangeExpandTransform.of("(\\d+) to (\\d+)", 5, 10)));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with every slot set round-trips to the same output")
    void wireRoundTripConfiguredExecutes() {
        DataPipeline<List<Integer>> original = pipeline("Levels 0 to 10", RangeExpandTransform.of("(\\d+) to (\\d+)", 5, 10));
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(original)).execute(), is(equalTo(original.execute())));
    }

    @Test
    @DisplayName("A wire file with a step of zero fails to load")
    void wireRefusesZeroStep() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"1-3\"},"
            + "{\"kind\":\"TRANSFORM_RANGE_EXPAND\",\"step\":0}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("RangeExpandTransform step '0'"));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

}
