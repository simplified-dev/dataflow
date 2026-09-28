package dev.simplified.dataflow.stage;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataPipelineResolver;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.fixture.AppendOperandTransform;
import dev.simplified.dataflow.stage.meta.StageMetadata;
import dev.simplified.dataflow.stage.meta.StageReflection;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the {@link FieldSpec.Type#PIPELINE} slot: reflection, {@link StageConfig},
 * {@link FieldSpec} accessors, the wire form and its validation, through the test-only
 * {@link AppendOperandTransform}.
 */
class PipelineSlotTest {

    private static final @NotNull StageMetadata METADATA = StageReflection.of(AppendOperandTransform.class);

    private static @NotNull DataPipeline<String> text(@NotNull String value) {
        return DataPipeline.builder().source(LiteralSource.text(value)).build();
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    @Test
    @DisplayName("Reflection maps a DataPipeline<?> factory parameter to a PIPELINE slot")
    void reflectionMapsPipelineParameter() {
        assertThat(METADATA.schema().getFirst().type(), is(FieldSpec.Type.PIPELINE));
    }

    @Test
    @DisplayName("StageConfig.pipeline stores the operand and getPipeline returns the same instance")
    void stageConfigStoresPipeline() {
        DataPipeline<String> operand = text("x");
        StageConfig cfg = StageConfig.builder().pipeline("suffix", operand).build();
        assertThat(cfg.getPipeline("suffix"), is(sameInstance(operand)));
    }

    @Test
    @DisplayName("FieldSpec.put and get route a PIPELINE slot through StageConfig")
    @SuppressWarnings("unchecked")
    void fieldSpecPutGet() {
        FieldSpec<DataPipeline<?>> spec = (FieldSpec<DataPipeline<?>>) METADATA.schema().getFirst();
        DataPipeline<String> operand = text("x");
        StageConfig cfg = spec.put(StageConfig.builder(), operand).build();
        assertThat(spec.get(cfg), is(sameInstance(operand)));
    }

    @Test
    @DisplayName("config() carries the operand instance the stage was built with")
    void configCarriesOperand() {
        DataPipeline<String> operand = text("x");
        assertThat(AppendOperandTransform.of(operand).config().getPipeline("suffix"), is(sameInstance(operand)));
    }

    @Test
    @DisplayName("fromConfig rebuilds the stage around the configured operand")
    void fromConfigRebuilds() {
        DataPipeline<String> operand = text("x");
        Stage<?, ?> rebuilt = METADATA.fromConfig(AppendOperandTransform.of(operand).config());
        assertThat(((AppendOperandTransform) rebuilt).suffix(), is(sameInstance(operand)));
    }

    @Test
    @DisplayName("writeJson writes the operand as a stage array opening with its source")
    void writeJsonWritesStageArray() {
        FieldSpec<?> spec = METADATA.schema().getFirst();
        JsonElement written = spec.writeJson(text("x"), stage -> {
            JsonObject o = new JsonObject();
            o.addProperty("kind", stage.kindId());
            return o;
        });
        assertThat(((JsonArray) written).get(0).getAsJsonObject().get("kind").getAsString(), is(equalTo("SOURCE_LITERAL")));
    }

    @Test
    @DisplayName("The wire form nests the operand as a pipeline-file stage array")
    void wireFormNestsStageArray() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(AppendOperandTransform.of(text("b")))
            .build();
        assertThat(PipelineGson.toJson(pipeline), containsString(
            "\"suffix\":[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"b\"}]"
        ));
    }

    @Test
    @DisplayName("A pipeline with an operand round-trips to the same JSON")
    void roundTripIsStable() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(AppendOperandTransform.of(text("b")))
            .build();
        String first = PipelineGson.toJson(pipeline);
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with an operand round-trips to the same output")
    void roundTripExecutes() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("a"))
            .stage(AppendOperandTransform.of(text("b")))
            .build();
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo("ab")));
    }

    @Test
    @DisplayName("A single SOURCE_EMBED operand on the wire runs the saved pipeline")
    void embedOperandFromWire() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TEST_APPEND_OPERAND\",\"suffix\":[{\"kind\":\"SOURCE_EMBED\",\"embeddedPipelineId\":\"saved\",\"outputType\":\"STRING\"}]}]";
        DataPipelineResolver resolver = new DataPipelineResolver() {
            @Override
            public @NotNull Optional<DataPipeline<?>> resolve(@NotNull String id) {
                return "saved".equals(id) ? Optional.of(text("z")) : Optional.empty();
            }

            @Override
            public @Nullable String idOf(@NotNull DataPipeline<?> pipeline) {
                return null;
            }
        };
        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();
        assertThat(PipelineGson.fromJson(json).execute(ctx), is(equalTo("az")));
    }

    @Test
    @DisplayName("An operand whose stage 0 is not a source fails the load")
    void sourcelessOperandRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TEST_APPEND_OPERAND\",\"suffix\":[{\"kind\":\"TRANSFORM_TRIM\"}]}]";
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> PipelineGson.fromJson(json));
        assertThat(thrown.getMessage(), containsString("First stage must be a SourceStage"));
    }

    @Test
    @DisplayName("An operand whose type chain breaks fails the load")
    void brokenOperandChainRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TEST_APPEND_OPERAND\",\"suffix\":["
            + "{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"b\"},"
            + "{\"kind\":\"TRANSFORM_JSON_STRINGIFY\"}]}]";
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> PipelineGson.fromJson(json));
        assertThat(thrown.getMessage(), containsString("Cannot build invalid pipeline"));
    }

    @Test
    @DisplayName("An operand of the wrong output type fails the load with the factory's message")
    void wrongOperandTypeRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TEST_APPEND_OPERAND\",\"suffix\":[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"INT\",\"value\":\"3\"}]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), containsString("Invalid AppendOperandTransform operand"));
    }

    @Test
    @DisplayName("An empty operand array fails the load: an empty pipeline has no source")
    void emptyOperandRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TEST_APPEND_OPERAND\",\"suffix\":[]}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), containsString("Pipeline has no stages"));
    }

    @Test
    @DisplayName("The factory rejects an operand of the wrong output type")
    void factoryRejectsWrongType() {
        DataPipeline<Integer> operand = DataPipeline.builder().source(LiteralSource.integerVal(3)).build();
        assertThrows(IllegalArgumentException.class, () -> AppendOperandTransform.of(operand));
    }

    @Test
    @DisplayName("DataPipeline.validate(expected) accepts a pipeline producing the expected type")
    void validateExpectedAccepts() {
        assertThat(text("x").validate(DataTypes.STRING).isValid(), is(true));
    }

    @Test
    @DisplayName("DataPipeline.validate(expected) reports a pipeline producing another type")
    void validateExpectedReportsMismatch() {
        assertThat(text("x").validate(DataTypes.INT).issues().getFirst().message(),
            containsString("Pipeline produces STRING but caller expected INT"));
    }

    @Test
    @DisplayName("DataPipeline.validate(expected) reports an empty pipeline as sourceless")
    void validateExpectedReportsEmpty() {
        assertThat(DataPipeline.empty().validate(DataTypes.STRING).issues().getFirst().message(),
            containsString("Pipeline has no stages"));
    }

}
