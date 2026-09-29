package dev.simplified.dataflow.stage.filter.list;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.simplified.dataflow.stage.predicate.string.NonEmptyPredicate;
import dev.simplified.dataflow.stage.predicate.string.StartsWithPredicate;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WhereFilterTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL_LIST\",\"elementType\":\"STRING\",\"value\":\"[\\\"a\\\"]\"}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull WhereFilter<String> startingWithB() {
        return WhereFilter.of(DataTypes.STRING, List.of(StartsWithPredicate.of("b")));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<String> fruit() {
        return List.of("apple", "banana", "cherry", "blueberry", "avocado");
    }

    private static @NotNull DataPipeline<?> pipeline() {
        return DataPipeline.builder()
            .source(LiteralListSource.strings(fruit().toArray(String[]::new)))
            .stage(startingWithB())
            .build();
    }

    @Test
    @DisplayName("Keeps the elements whose body yields true, in input order")
    void keepsTrueInOrder() {
        assertThat(startingWithB().execute(this.ctx, fruit()), contains("banana", "blueberry"));
    }

    @Test
    @DisplayName("An element whose body yields null is dropped")
    void nullVerdictDropped() {
        WhereFilter<String> stage = WhereFilter.of(
            DataTypes.STRING, List.of(RegexExtractTransform.of("^\\d+"), NonEmptyPredicate.of())
        );
        assertThat(stage.execute(this.ctx, List.of("12a", "b", "3", "c4")), contains("12a", "3"));
    }

    @Test
    @DisplayName("A null element is dropped")
    void nullElementDropped() {
        assertThat(startingWithB().execute(this.ctx, Arrays.asList("bee", null, "boat")), contains("bee", "boat"));
    }

    @Test
    @DisplayName("A null element does not run the body")
    void nullElementSkipsBody() {
        StartsWithPredicate watched = StartsWithPredicate.of("b");
        WhereFilter<String> stage = WhereFilter.of(DataTypes.STRING, List.of(watched));
        AtomicInteger runs = new AtomicInteger();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((ran, output) -> {
                if (ran == watched) runs.incrementAndGet();
            })
            .build();

        stage.execute(counting, Arrays.asList("bee", null));

        assertThat(runs.get(), is(1));
    }

    @Test
    @DisplayName("The body runs once per element")
    void bodyRunsOncePerElement() {
        StartsWithPredicate watched = StartsWithPredicate.of("b");
        WhereFilter<String> stage = WhereFilter.of(DataTypes.STRING, List.of(watched));
        AtomicInteger runs = new AtomicInteger();
        PipelineContext counting = PipelineContext.builder()
            .withTrace((ran, output) -> {
                if (ran == watched) runs.incrementAndGet();
            })
            .build();

        stage.execute(counting, fruit());

        assertThat(runs.get(), is(5));
    }

    @Test
    @DisplayName("A multi-stage body tests a derived value")
    void derivedValueBody() {
        WhereFilter<String> stage = WhereFilter.of(
            DataTypes.STRING, List.of(LengthTransform.of(), IntGreaterThanPredicate.of(5))
        );
        assertThat(stage.execute(this.ctx, fruit()), contains("banana", "cherry", "blueberry", "avocado"));
    }

    @Test
    @DisplayName("No element passing yields an empty list")
    void nonePassYieldsEmpty() {
        assertThat(startingWithB().execute(this.ctx, List.of("apple", "cherry")), is(empty()));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(startingWithB().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("The result is an unmodifiable list")
    void resultIsUnmodifiable() {
        List<String> result = startingWithB().execute(this.ctx, fruit());
        assertThrows(UnsupportedOperationException.class, () -> result.add("x"));
    }

    @Test
    @DisplayName("Input and output are the element list type")
    void typesAreElementList() {
        assertThat(startingWithB().outputType().label(), is(equalTo("List<STRING>")));
    }

    @Test
    @DisplayName("of refuses a body that does not produce BOOLEAN")
    void rejectsMistypedBody() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> WhereFilter.of(
            DataTypes.STRING, List.of(UpperCaseTransform.of())
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid WhereFilter body"));
    }

    @Test
    @DisplayName("of refuses a body that does not consume the element type")
    void rejectsBodyOfOtherInput() {
        assertThrows(IllegalArgumentException.class, () -> WhereFilter.of(
            DataTypes.INT, List.of(StartsWithPredicate.of("b"))
        ));
    }

    @Test
    @DisplayName("of refuses an empty body")
    void rejectsEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> WhereFilter.of(DataTypes.STRING, List.of()));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> pipeline = pipeline();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A wire stage reads its elementType and body keys")
    void wireReadsKeys() {
        String json = "[" + SOURCE + ",{\"kind\":\"FILTER_WHERE\",\"elementType\":\"STRING\","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"a\"}]}]";
        assertThat(PipelineGson.fromJson(json).execute(), is(equalTo(List.of("a"))));
    }

    @Test
    @DisplayName("A wire body that does not produce BOOLEAN fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"FILTER_WHERE\",\"elementType\":\"STRING\","
            + "\"body\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid WhereFilter body"));
    }

}
