package dev.simplified.dataflow.stage.filter.list;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DistinctByFilterTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL_LIST\",\"elementType\":\"STRING\",\"value\":\"[\\\"a\\\"]\"}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull DistinctByFilter<String, String> byInitial(@Nullable Boolean keepLast) {
        return DistinctByFilter.of(DataTypes.STRING, DataTypes.STRING, List.of(RegexExtractTransform.of("^\\d")), keepLast);
    }

    private static @NotNull DistinctByFilter<String, String> byFirstLetter(@Nullable Boolean keepLast) {
        return DistinctByFilter.of(DataTypes.STRING, DataTypes.STRING, List.of(RegexExtractTransform.of("^.")), keepLast);
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull List<String> fruit() {
        return List.of("apple", "bob", "avocado", "cat", "banana");
    }

    @Test
    @DisplayName("Keeps the first element per key, in input order")
    void keepsFirstPerKey() {
        assertThat(byFirstLetter(null).execute(this.ctx, fruit()), contains("apple", "bob", "cat"));
    }

    @Test
    @DisplayName("keepLast keeps the last element per key, in input order")
    void keepLastKeepsLastPerKey() {
        assertThat(byFirstLetter(true).execute(this.ctx, fruit()), contains("avocado", "cat", "banana"));
    }

    @Test
    @DisplayName("An explicit keepLast of false keeps the first element per key")
    void keepLastFalseKeepsFirst() {
        assertThat(byFirstLetter(false).execute(this.ctx, fruit()), contains("apple", "bob", "cat"));
    }

    @Test
    @DisplayName("An element whose key body yields null is dropped")
    void nullKeyDropped() {
        assertThat(byInitial(null).execute(this.ctx, List.of("1x", "yy", "2z", "1w")), contains("1x", "2z"));
    }

    @Test
    @DisplayName("A null element is dropped, its key being null")
    void nullElementDropped() {
        assertThat(byFirstLetter(null).execute(this.ctx, Arrays.asList("a", null, "b")), contains("a", "b"));
    }

    @Test
    @DisplayName("An INT key compares by value")
    void intKey() {
        DistinctByFilter<String, Integer> stage = DistinctByFilter.of(DataTypes.STRING, DataTypes.INT, List.of(LengthTransform.of()), null);
        assertThat(stage.execute(this.ctx, List.of("a", "bb", "c", "dd", "eee")), contains("a", "bb", "eee"));
    }

    @Test
    @DisplayName("Null in, null out")
    void nullInputProducesNull() {
        assertThat(byFirstLetter(null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("An empty list stays empty")
    void emptyStaysEmpty() {
        assertThat(byFirstLetter(null).execute(this.ctx, List.of()), is(empty()));
    }

    @Test
    @DisplayName("The result is a new unmodifiable list")
    void resultIsUnmodifiable() {
        List<String> result = byFirstLetter(null).execute(this.ctx, fruit());
        assertThrows(UnsupportedOperationException.class, () -> result.add("x"));
    }

    @Test
    @DisplayName("Input and output are the element list type")
    void typesAreElementList() {
        assertThat(byFirstLetter(null).outputType().label(), is(equalTo("List<STRING>")));
    }

    @Test
    @DisplayName("of rejects a key type outside the comparable keys")
    void rejectsUnsupportedKeyType() {
        assertThrows(IllegalArgumentException.class, () -> DistinctByFilter.of(
            DataTypes.STRING, DataTypes.BOOLEAN, List.of(RegexExtractTransform.of("^.")), null
        ));
    }

    @Test
    @DisplayName("of rejects a body that does not produce the key type")
    void rejectsMistypedBody() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> DistinctByFilter.of(
            DataTypes.STRING, DataTypes.INT, List.of(UpperCaseTransform.of()), null
        ));
        assertThat(thrown.getMessage(), startsWith("Invalid DistinctByFilter body"));
    }

    @Test
    @DisplayName("of rejects an empty body")
    void rejectsEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> DistinctByFilter.of(DataTypes.STRING, DataTypes.STRING, List.of(), null));
    }

    @Test
    @DisplayName("An absent keepLast stays absent from the config")
    void absentKeepLastStaysAbsent() {
        assertThat(byFirstLetter(null).config().has("keepLast"), is(false));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same JSON")
    void wireRoundTripIsStable() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings(fruit().toArray(String[]::new)))
            .stage(byFirstLetter(true))
            .build();
        String first = PipelineGson.toJson(pipeline);
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips through the wire to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings(fruit().toArray(String[]::new)))
            .stage(byFirstLetter(true))
            .build();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

    @Test
    @DisplayName("A wire body that does not produce the key type fails at load")
    void wireMistypedBodyFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"FILTER_DISTINCT_BY\",\"elementType\":\"STRING\",\"keyType\":\"INT\","
            + "\"body\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid DistinctByFilter body"));
    }

}
