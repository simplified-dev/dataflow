package dev.simplified.dataflow.stage.transform.primitive;

import com.google.gson.JsonElement;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.predicate.string.StartsWithPredicate;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.ParseJsonTransform;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.SplitTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CoalesceTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    /**
     * Yields the digits of its input, or {@code null} when it has none.
     */
    private static @NotNull List<Stage<?, ?>> digits() {
        return List.of(RegexExtractTransform.of("\\d+"));
    }

    /**
     * Yields the letters of its input, or {@code null} when it has none.
     */
    private static @NotNull List<Stage<?, ?>> letters() {
        return List.of(RegexExtractTransform.of("[a-z]+"));
    }

    /**
     * Yields {@code true} for an input starting with {@code y}, {@code false} for any other input
     * with a letter in it, and {@code null} for an input without one.
     */
    private static @NotNull List<Stage<?, ?>> startsWithY() {
        return List.of(RegexExtractTransform.of("[a-z].*"), StartsWithPredicate.of("y"));
    }

    /**
     * Replaces its input with {@code literal} and parses that as JSON.
     */
    private static @NotNull List<Stage<?, ?>> json(@NotNull String literal) {
        return List.of(
            ConstantTransform.of(DataTypes.STRING, DataTypes.STRING, literal),
            ToRawTransform.of(DataTypes.RAW_JSON),
            ParseJsonTransform.of()
        );
    }

    private static @NotNull CoalesceTransform<String, String> coalesce(
        @Nullable List<Stage<?, ?>> fallback,
        @Nullable String defaultValue,
        @Nullable List<Stage<?, ?>> when
    ) {
        return CoalesceTransform.of(DataTypes.STRING, DataTypes.STRING, digits(), fallback, defaultValue, when);
    }

    private @Nullable String run(@NotNull CoalesceTransform<String, String> stage, @Nullable String input) {
        return stage.execute(this.ctx, input);
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull DataPipeline<List<String>> everySlot() {
        return DataPipeline.builder()
            .source(LiteralSource.text("yes12,yes,no7,!"))
            .stage(SplitTransform.of(","))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.STRING, List.of(
                CoalesceTransform.of(DataTypes.STRING, DataTypes.STRING, digits(), letters(), "none", startsWithY())
            )))
            .build();
    }

    @Test
    @DisplayName("A null input stays null, whatever the default")
    void nullInNullOut() {
        assertThat(run(coalesce(letters(), "none", null), null), is(nullValue()));
    }

    @Test
    @DisplayName("A body that yields a value wins")
    void bodyWins() {
        assertThat(run(coalesce(letters(), "none", null), "ab12"), is(equalTo("12")));
    }

    @Test
    @DisplayName("The fallback answers when the body yields null")
    void fallbackAfterNullBody() {
        assertThat(run(coalesce(letters(), "none", null), "ab"), is(equalTo("ab")));
    }

    @Test
    @DisplayName("The default answers when the body and the fallback yield null")
    void defaultAfterNullFallback() {
        assertThat(run(coalesce(letters(), "none", null), "!"), is(equalTo("none")));
    }

    @Test
    @DisplayName("The default answers a null body when no fallback is configured")
    void defaultWithoutFallback() {
        assertThat(run(coalesce(null, "none", null), "ab"), is(equalTo("none")));
    }

    @Test
    @DisplayName("Without a default the stage rejects with null when every body yields null")
    void nullWhenEveryAlternativeMisses() {
        assertThat(run(coalesce(letters(), null, null), "!"), is(nullValue()));
    }

    @Test
    @DisplayName("The fallback does not run when the body yields a value")
    void fallbackNotRunAfterValue() {
        AtomicInteger fallbackRuns = new AtomicInteger();
        List<Stage<?, ?>> fallback = letters();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((stage, out) -> {
                if (stage == fallback.getFirst()) fallbackRuns.incrementAndGet();
            })
            .build();
        coalesce(fallback, null, null).execute(counting, "ab12");
        assertThat(fallbackRuns.get(), is(equalTo(0)));
    }

    @Test
    @DisplayName("A when body yielding true lets the body run")
    void whenTrueRunsBody() {
        assertThat(run(coalesce(letters(), "none", startsWithY()), "yes7"), is(equalTo("7")));
    }

    @Test
    @DisplayName("A when body yielding false skips the body for the fallback")
    void whenFalseSkipsBody() {
        assertThat(run(coalesce(letters(), "none", startsWithY()), "no7"), is(equalTo("no")));
    }

    @Test
    @DisplayName("A when body yielding false skips the body for the default when there is no fallback")
    void whenFalseSkipsBodyForDefault() {
        assertThat(run(coalesce(null, "none", startsWithY()), "no7"), is(equalTo("none")));
    }

    @Test
    @DisplayName("A when body yielding null skips the body")
    void whenNullSkipsBody() {
        assertThat(run(coalesce(null, "none", startsWithY()), "7"), is(equalTo("none")));
    }

    @Test
    @DisplayName("A when body yielding true still falls through a null body")
    void whenTrueNullBodyFallsThrough() {
        assertThat(run(coalesce(letters(), "none", startsWithY()), "yes"), is(equalTo("yes")));
    }

    @Test
    @DisplayName("A second coalesce as the fallback gives a third alternative")
    void nestedFallback() {
        CoalesceTransform<String, String> inner = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.STRING, letters(), null, "none", null
        );
        CoalesceTransform<String, String> outer = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.STRING, digits(), List.of(inner), null, null
        );
        assertThat(run(outer, "!"), is(equalTo("none")));
    }

    @Test
    @DisplayName("A non-string default is parsed under the output type")
    void defaultParsedUnderOutputType() {
        CoalesceTransform<String, Integer> stage = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.INT, List.of(RegexExtractTransform.of("\\d+"), ParseIntTransform.of()), null, " 0 ", null
        );
        assertThat(stage.execute(this.ctx, "none"), is(equalTo(0)));
    }

    @Test
    @DisplayName("The default is parsed once, when the stage is built")
    void defaultParsedOnce() {
        CoalesceTransform<String, Long> stage = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.LONG, List.of(RegexExtractTransform.of("\\d+"), ParseLongTransform.of()), null, "9000000000", null
        );
        assertThat(stage.execute(this.ctx, "a"), is(sameInstance(stage.execute(this.ctx, "b"))));
    }

    @Test
    @DisplayName("of refuses a stage with neither a fallback nor a default")
    void refusesNoAlternative() {
        assertThrows(IllegalArgumentException.class, () -> coalesce(null, null, null));
    }

    @Test
    @DisplayName("of refuses a body that does not produce the output type")
    void refusesMistypedBody() {
        assertThrows(IllegalArgumentException.class, () -> CoalesceTransform.of(
            DataTypes.STRING, DataTypes.INT, digits(), null, "0", null
        ));
    }

    @Test
    @DisplayName("of refuses an empty body")
    void refusesEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> CoalesceTransform.of(
            DataTypes.STRING, DataTypes.STRING, List.of(), null, "none", null
        ));
    }

    @Test
    @DisplayName("of refuses a fallback that does not produce the output type")
    void refusesMistypedFallback() {
        assertThrows(IllegalArgumentException.class, () -> CoalesceTransform.of(
            DataTypes.STRING, DataTypes.STRING, digits(), List.of(RegexExtractTransform.of("\\d+"), ParseIntTransform.of()), null, null
        ));
    }

    @Test
    @DisplayName("of refuses a when body that does not produce BOOLEAN")
    void refusesMistypedWhen() {
        assertThrows(IllegalArgumentException.class, () -> coalesce(null, "none", letters()));
    }

    @Test
    @DisplayName("of refuses a default that does not parse as the output type")
    void refusesUnparsableDefault() {
        assertThrows(IllegalArgumentException.class, () -> CoalesceTransform.of(
            DataTypes.STRING, DataTypes.INT, List.of(RegexExtractTransform.of("\\d+"), ParseIntTransform.of()), null, "x", null
        ));
    }

    @Test
    @DisplayName("A structured output type builds with a fallback alone")
    void structuredOutputWithFallback() {
        CoalesceTransform<String, JsonElement> stage = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.JSON_ELEMENT, json("{}"), json("[]"), null, null
        );
        assertThat(stage.outputType(), is(sameInstance(DataTypes.JSON_ELEMENT)));
    }

    @Test
    @DisplayName("of refuses a default for an output type SOURCE_LITERAL does not admit")
    void refusesDefaultForStructuredOutput() {
        assertThrows(IllegalArgumentException.class, () -> CoalesceTransform.of(
            DataTypes.STRING, DataTypes.JSON_ELEMENT, json("{}"), null, "{}", null
        ));
    }

    @Test
    @DisplayName("config() carries the default as configured, not the parsed value")
    void configCarriesRawDefault() {
        CoalesceTransform<String, Integer> stage = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.INT, List.of(RegexExtractTransform.of("\\d+"), ParseIntTransform.of()), null, " 0 ", null
        );
        assertThat(stage.config().getString("defaultValue"), is(equalTo(" 0 ")));
    }

    @Test
    @DisplayName("Inside a map body every element takes its first present alternative")
    void everySlotInBody() {
        assertThat(everySlot().execute(this.ctx), contains("12", "yes", "no", "none"));
    }

    @Test
    @DisplayName("A pipeline using every slot round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(everySlot());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline using every slot round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(everySlot()));
        assertThat(rebuilt.execute(this.ctx), is(equalTo(everySlot().execute(this.ctx))));
    }

    @Test
    @DisplayName("The wire form omits the optional slots left out")
    void wireFormOmitsAbsentSlots() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(coalesce(null, "none", null))
            .build();
        assertThat(PipelineGson.toJson(pipeline), allOf(not(containsString("\"fallback\"")), not(containsString("\"when\""))));
    }

    @Test
    @DisplayName("A stage with neither a fallback nor a default fails the load")
    void wireRejectsNoAlternative() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TRANSFORM_COALESCE\",\"inputType\":\"STRING\",\"outputType\":\"STRING\","
            + "\"body\":[{\"kind\":\"TRANSFORM_REGEX_EXTRACT\",\"regex\":\"\\\\d+\",\"group\":0}]}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

}
