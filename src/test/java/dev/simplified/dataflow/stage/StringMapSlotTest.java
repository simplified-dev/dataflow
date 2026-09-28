package dev.simplified.dataflow.stage;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.fixture.LookupTableTransform;
import dev.simplified.dataflow.stage.meta.StageMetadata;
import dev.simplified.dataflow.stage.meta.StageReflection;
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
 * Covers the {@link FieldSpec.Type#STRING_MAP} slot: reflection, {@link StageConfig},
 * {@link FieldSpec} accessors and the wire form, through the test-only
 * {@link LookupTableTransform}.
 */
class StringMapSlotTest {

    private static final @NotNull StageMetadata METADATA = StageReflection.of(LookupTableTransform.class);

    @SuppressWarnings("unchecked")
    private static @NotNull FieldSpec<Map<String, String>> spec() {
        return (FieldSpec<Map<String, String>>) METADATA.schema().getFirst();
    }

    private static @NotNull Map<String, String> zam() {
        Map<String, String> table = new LinkedHashMap<>();
        table.put("z", "1");
        table.put("a", "2");
        table.put("m", "3");
        return table;
    }

    private static @NotNull Map<String, String> read(@NotNull String json) {
        JsonElement raw = JsonParser.parseString(json);
        StageConfig cfg = spec().readJson(raw, StageConfig.builder(), o -> {
            throw new AssertionError("no stages in a STRING_MAP");
        }).build();
        return cfg.getStringMap("table");
    }

    @Test
    @DisplayName("Reflection maps a Map<String, String> factory parameter to a STRING_MAP slot")
    void reflectionMapsStringMapParameter() {
        assertThat(spec().type(), is(FieldSpec.Type.STRING_MAP));
    }

    @Test
    @DisplayName("StageConfig.stringMap keeps the insertion order")
    void stageConfigKeepsOrder() {
        StageConfig cfg = StageConfig.builder().stringMap("table", zam()).build();
        assertThat(List.copyOf(cfg.getStringMap("table").keySet()), contains("z", "a", "m"));
    }

    @Test
    @DisplayName("StageConfig.stringMap stores a copy the caller's later changes do not reach")
    void stageConfigCopies() {
        Map<String, String> table = zam();
        StageConfig cfg = StageConfig.builder().stringMap("table", table).build();
        table.put("late", "x");
        assertThat(cfg.getStringMap("table"), not(hasKey("late")));
    }

    @Test
    @DisplayName("StageConfig.stringMap stores an unmodifiable map")
    void stageConfigIsUnmodifiable() {
        StageConfig cfg = StageConfig.builder().stringMap("table", zam()).build();
        assertThrows(UnsupportedOperationException.class, () -> cfg.getStringMap("table").put("k", "v"));
    }

    @Test
    @DisplayName("FieldSpec.put and get route a STRING_MAP slot through StageConfig")
    void fieldSpecPutGet() {
        StageConfig cfg = spec().put(StageConfig.builder(), zam()).build();
        assertThat(spec().get(cfg), is(equalTo(zam())));
    }

    @Test
    @DisplayName("writeJson writes a JSON object of strings in insertion order")
    void writeJsonKeepsOrder() {
        JsonElement written = spec().writeJson(zam(), stage -> {
            throw new AssertionError("no stages in a STRING_MAP");
        });
        assertThat(written.toString(), is(equalTo("{\"z\":\"1\",\"a\":\"2\",\"m\":\"3\"}")));
    }

    @Test
    @DisplayName("readJson reads a JSON object of strings in document order")
    void readJsonKeepsOrder() {
        assertThat(List.copyOf(read("{\"z\":\"1\",\"a\":\"2\",\"m\":\"3\"}").keySet()), contains("z", "a", "m"));
    }

    @Test
    @DisplayName("readJson reads a primitive value as its string form, as a STRING slot does")
    void readJsonReadsPrimitiveAsString() {
        assertThat(read("{\"a\":1}").get("a"), is(equalTo("1")));
    }

    @Test
    @DisplayName("readJson rejects a JSON null value")
    void readJsonRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> read("{\"a\":null}"));
    }

    @Test
    @DisplayName("readJson rejects an object value")
    void readJsonRejectsObject() {
        assertThrows(IllegalArgumentException.class, () -> read("{\"a\":{\"b\":\"c\"}}"));
    }

    @Test
    @DisplayName("config() and fromConfig round-trip the table in order")
    void configRoundTrip() {
        Stage<?, ?> rebuilt = METADATA.fromConfig(LookupTableTransform.of(zam()).config());
        assertThat(List.copyOf(((LookupTableTransform) rebuilt).table().keySet()), contains("z", "a", "m"));
    }

    @Test
    @DisplayName("A pipeline with a STRING_MAP round-trips to the same JSON, keys in order")
    void wireRoundTripIsStable() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(LookupTableTransform.of(zam()))
            .build();
        String first = PipelineGson.toJson(pipeline);
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The wire form is a JSON object in the table's order")
    void wireFormIsOrderedObject() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(LookupTableTransform.of(zam()))
            .build();
        assertThat(PipelineGson.toJson(pipeline), containsString("\"table\":{\"z\":\"1\",\"a\":\"2\",\"m\":\"3\"}"));
    }

    @Test
    @DisplayName("A pipeline with a STRING_MAP round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(LookupTableTransform.of(zam()))
            .build();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo("2")));
    }

}
