package dev.simplified.dataflow.stage.transform.primitive;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.exception.ExpectationFailedException;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.fixture.AppendOperandTransform;
import dev.simplified.dataflow.stage.predicate.common.NotNullPredicate;
import dev.simplified.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.simplified.dataflow.stage.predicate.string.StartsWithPredicate;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.json.PathTransform;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExpectTransformTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"item_42\"}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull ExpectTransform<String> itemId() {
        return ExpectTransform.of(DataTypes.STRING, "the id is an item id", List.of(StartsWithPredicate.of("item_")));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull DataPipeline<String> pipeline(@NotNull String value) {
        return DataPipeline.builder()
            .source(LiteralSource.text(value))
            .stage(itemId())
            .stage(UpperCaseTransform.of())
            .build();
    }

    @Test
    @DisplayName("A value whose body yields true passes through as the same instance")
    void passingValueIsReturned() {
        String value = "item_1";
        assertThat(itemId().execute(this.ctx, value), is(sameInstance(value)));
    }

    @Test
    @DisplayName("A value whose body yields false throws")
    void falseVerdictThrows() {
        ExpectTransform<String> stage = itemId();
        assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "mob_1"));
    }

    @Test
    @DisplayName("The failure message names the expectation")
    void failureNamesExpectation() {
        ExpectTransform<String> stage = itemId();
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "mob_1"));
        assertThat(thrown.getMessage(), startsWith("Expectation 'the id is an item id' failed"));
    }

    @Test
    @DisplayName("The failure message carries the failing value")
    void failureCarriesValue() {
        ExpectTransform<String> stage = itemId();
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "mob_1"));
        assertThat(thrown.getMessage(), containsString("'mob_1'"));
    }

    @Test
    @DisplayName("The failure message shortens a long value")
    void failureShortensLongValue() {
        String value = "x".repeat(500);
        ExpectTransform<String> stage = itemId();
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, value));
        assertThat(thrown.getMessage(), not(containsString(value)));
    }

    @Test
    @DisplayName("A value whose body yields null throws")
    void nullVerdictThrows() {
        ExpectTransform<String> stage = ExpectTransform.of(
            DataTypes.STRING, "the value carries a number", List.of(RegexExtractTransform.of("\\d+"), ParseIntTransform.of(), IntGreaterThanPredicate.of(0))
        );
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "none"));
        assertThat(thrown.getMessage(), containsString("body yielded 'null'"));
    }

    @Test
    @DisplayName("A null input passes through as null")
    void nullInputPassesThrough() {
        assertThat(itemId().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A null input does not run the body")
    void nullInputSkipsBody() {
        StartsWithPredicate watched = StartsWithPredicate.of("item_");
        ExpectTransform<String> stage = ExpectTransform.of(DataTypes.STRING, "the id is an item id", List.of(watched));
        AtomicInteger runs = new AtomicInteger();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((ran, output) -> {
                if (ran == watched) runs.incrementAndGet();
            })
            .build();

        stage.execute(counting, null);

        assertThat(runs.get(), is(0));
    }

    @Test
    @DisplayName("A failing expectation stops the pipeline run")
    void failureStopsRun() {
        DataPipeline<String> pipeline = pipeline("mob_1");
        assertThrows(ExpectationFailedException.class, pipeline::execute);
    }

    @Test
    @DisplayName("A passing expectation lets the run continue")
    void passLetsRunContinue() {
        assertThat(pipeline("item_7").execute(), is(equalTo("ITEM_7")));
    }

    @Test
    @DisplayName("One failing element inside a map body fails the whole run")
    void failingElementInBodyFailsRun() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings("item_1", "mob_2", "item_3"))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.STRING, List.of(itemId())))
            .build();
        assertThrows(ExpectationFailedException.class, pipeline::execute);
    }

    @Test
    @DisplayName("A body reading a field the row lacks yields null, which fails the expectation")
    void missingFieldFailsExpectation() {
        ExpectTransform<JsonElement> stage = ExpectTransform.of(
            DataTypes.JSON_ELEMENT, "every row carries an id", List.of(PathTransform.of("id"), NotNullPredicate.of(DataTypes.JSON_ELEMENT))
        );
        JsonElement row = JsonParser.parseString("{\"name\":\"apple\"}");
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, row));
        assertThat(thrown.getMessage(), containsString("body yielded 'null'"));
    }

    @Test
    @DisplayName("An expectation whose body tests presence passes a null input through without failing")
    void presenceBodyPassesNullInput() {
        ExpectTransform<String> stage = ExpectTransform.of(DataTypes.STRING, "the id is present", List.of(NotNullPredicate.of(DataTypes.STRING)));
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("The failure message names the input type")
    void failureNamesInputType() {
        ExpectTransform<String> stage = itemId();
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "mob_1"));
        assertThat(thrown.getMessage(), endsWith("of type 'STRING'"));
    }

    @Test
    @DisplayName("An expectation carrying format characters is named verbatim")
    void formatCharactersKeptVerbatim() {
        ExpectTransform<String> stage = ExpectTransform.of(DataTypes.STRING, "100% of ids %s start with item_", List.of(StartsWithPredicate.of("item_")));
        ExpectationFailedException thrown = assertThrows(ExpectationFailedException.class, () -> stage.execute(this.ctx, "mob_1"));
        assertThat(thrown.getMessage(), startsWith("Expectation '100% of ids %s start with item_' failed"));
    }

    @Test
    @DisplayName("A failing expectation inside a pipeline operand fails the consuming run with its own exception")
    void failureInOperandPropagates() {
        DataPipeline<String> suffix = DataPipeline.builder()
            .source(LiteralSource.text("_x"))
            .stage(itemId())
            .build();
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("item"))
            .stage(AppendOperandTransform.of(suffix))
            .build();
        assertThrows(ExpectationFailedException.class, pipeline::execute);
    }

    @Test
    @DisplayName("The output type is the input type")
    void outputTypeIsInputType() {
        assertThat(ExpectTransform.of(DataTypes.INT, "positive", List.of(IntGreaterThanPredicate.of(0))).outputType(), is(equalTo(DataTypes.INT)));
    }

    @Test
    @DisplayName("The summary names the expectation")
    void summaryNamesExpectation() {
        assertThat(itemId().summary(), is(equalTo("Expect 'the id is an item id'")));
    }

    @Test
    @DisplayName("of refuses a blank expectation")
    void refusesBlankExpectation() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ExpectTransform.of(
            DataTypes.STRING, "  ", List.of(StartsWithPredicate.of("item_"))
        ));
        assertThat(thrown.getMessage(), is(equalTo("ExpectTransform needs a non-blank expectation")));
    }

    @Test
    @DisplayName("of refuses an empty expectation")
    void refusesEmptyExpectation() {
        assertThrows(IllegalArgumentException.class, () -> ExpectTransform.of(
            DataTypes.STRING, "", List.of(StartsWithPredicate.of("item_"))
        ));
    }

    @Test
    @DisplayName("of refuses a body that does not produce BOOLEAN")
    void refusesMistypedBody() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ExpectTransform.of(
            DataTypes.STRING, "the id is an item id", List.of(LengthTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid ExpectTransform body"));
    }

    @Test
    @DisplayName("of refuses an empty body")
    void refusesEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> ExpectTransform.of(DataTypes.STRING, "the id is an item id", List.of()));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline("item_7"));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<String> pipeline = pipeline("item_7");
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A wire stage reads its inputType, expectation and body keys")
    void wireReadsKeys() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\",\"expectation\":\"an item\","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"item_\"}]}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo("item_42")));
    }

    @Test
    @DisplayName("A wire stage whose expectation fails throws on execute")
    void wireFailureThrows() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\",\"expectation\":\"a mob\","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"mob_\"}]}]";
        DataPipeline<?> pipeline = PipelineGson.fromJson(json);
        assertThrows(ExpectationFailedException.class, pipeline::execute);
    }

    @Test
    @DisplayName("A wire stage with a blank expectation fails at load")
    void wireBlankExpectationFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\",\"expectation\":\" \","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"item_\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), is(equalTo("ExpectTransform needs a non-blank expectation")));
    }

    @Test
    @DisplayName("A wire stage without an expectation fails at load")
    void wireMissingExpectationFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"item_\"}]}]";
        assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
    }

    @Test
    @DisplayName("A wire body that does not produce BOOLEAN fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\",\"expectation\":\"an item\","
            + "\"body\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid ExpectTransform body"));
    }

}
