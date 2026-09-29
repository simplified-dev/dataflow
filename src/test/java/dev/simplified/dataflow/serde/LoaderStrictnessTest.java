package dev.simplified.dataflow.serde;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.stage.StageConfig;
import dev.simplified.dataflow.stage.meta.StageMetadata;
import dev.simplified.dataflow.stage.meta.StageReflection;
import dev.simplified.dataflow.stage.source.UrlSource;
import dev.simplified.dataflow.stage.transform.primitive.CoalesceTransform;
import dev.simplified.dataflow.stage.transform.string.SplitTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers how strictly {@link PipelineGson#fromJson} and {@link StageMetadata#fromConfig}
 * read a stage: a required key that is absent, a key the stage does not declare and a JSON
 * {@code null} each fail the load or read as absent by rule, and a factory's own refusal reaches the
 * caller as the {@link IllegalArgumentException} it threw.
 */
class LoaderStrictnessTest {

    private static final @NotNull String SOURCE = "{'kind':'SOURCE_LITERAL','outputType':'STRING','value':'a,b'}";

    private static final @NotNull String LIST_SOURCE = "{'kind':'SOURCE_LITERAL_LIST','elementType':'STRING','value':'[\\'a\\']'}";

    private static @NotNull String q(@NotNull String json) {
        return json.replace('\'', '"');
    }

    private static @NotNull IllegalArgumentException loadFails(@NotNull String json) {
        return assertThrows(IllegalArgumentException.class, () -> PipelineGson.fromJson(q(json)));
    }

    @Nested
    @DisplayName("A required key")
    class RequiredKey {

        @Test
        @DisplayName("absent from a stage fails the load naming the stage and the key")
        void absentFailsNamingStageAndKey() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_SPLIT'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_SPLIT' is missing required key 'regex'")));
        }

        @Test
        @DisplayName("of a primitive type absent from a stage fails the load as a missing key")
        void absentPrimitiveFailsAsMissingKey() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'FILTER_TAKE','elementType':'STRING'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'FILTER_TAKE' is missing required key 'count'")));
        }

        @Test
        @DisplayName("renamed on the wire is reported under its wire name")
        void absentRenamedKeyReportedByWireName() {
            IllegalArgumentException thrown = loadFails("[{'kind':'SOURCE_LITERAL','outputType':'STRING'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'SOURCE_LITERAL' is missing required key 'value'")));
        }

        @Test
        @DisplayName("absent from a stage inside a body fails the load naming that stage")
        void absentInBodyFailsNamingBodyStage() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_MAP','elementInputType':'STRING',"
                + "'elementOutputType':'List<STRING>','body':[{'kind':'TRANSFORM_SPLIT'}]}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_SPLIT' is missing required key 'regex'")));
        }

        @Test
        @DisplayName("absent from a stage inside an operand fails the load naming that stage")
        void absentInOperandFailsNamingOperandStage() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_CONCAT','elementType':'STRING',"
                + "'other':[{'kind':'SOURCE_LITERAL_LIST','elementType':'STRING'}]}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'SOURCE_LITERAL_LIST' is missing required key 'value'")));
        }

        @Test
        @DisplayName("holding a JSON null fails the load as a missing key")
        void nullFailsAsMissingKey() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_SPLIT','regex':null}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_SPLIT' is missing required key 'regex'")));
        }

        @Test
        @DisplayName("absent from a configuration fails fromConfig naming the stage and the key")
        void absentFromConfigFails() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> StageReflection.of(SplitTransform.class).fromConfig(StageConfig.empty()));
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_SPLIT' is missing required key 'regex'")));
        }

    }

    @Nested
    @DisplayName("A key the stage does not declare")
    class UndeclaredKey {

        @Test
        @DisplayName("fails the load naming the stage, the key and the keys it declares")
        void failsNamingStageAndKey() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_SPLIT','regex':',','limit':2}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_SPLIT' does not declare key 'limit' (declared keys: [regex])")));
        }

        @Test
        @DisplayName("misspelling a required key is reported as the undeclared spelling")
        void misspeltRequiredKeyReported() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_SPLIT','regx':','}]");
            assertThat(thrown.getMessage(), startsWith("Stage 'TRANSFORM_SPLIT' does not declare key 'regx'"));
        }

        @Test
        @DisplayName("misspelling an optional key fails the load rather than reading as absent")
        void misspeltOptionalKeyFails() {
            IllegalArgumentException thrown = loadFails("[{'kind':'SOURCE_URL','outputType':'RAW_HTML','url':'https://example.com',"
                + "'maxBodyByte':1024}]");
            assertThat(thrown.getMessage(), startsWith("Stage 'SOURCE_URL' does not declare key 'maxBodyByte'"));
        }

        @Test
        @DisplayName("on a stage with no slots fails the load")
        void onSlotlessStageFails() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_TRIM','mode':'both'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage 'TRANSFORM_TRIM' does not declare key 'mode' (declared keys: none)")));
        }

        @Test
        @DisplayName("on a stage inside a body fails the load")
        void inBodyFails() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_MAP','elementInputType':'STRING',"
                + "'elementOutputType':'STRING','body':[{'kind':'TRANSFORM_TRIM','note':'x'}]}]");
            assertThat(thrown.getMessage(), startsWith("Stage 'TRANSFORM_TRIM' does not declare key 'note'"));
        }

        @Test
        @DisplayName("on a stage inside an operand fails the load")
        void inOperandFails() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_CONCAT','elementType':'STRING',"
                + "'other':[{'kind':'SOURCE_LITERAL_LIST','elementType':'STRING','value':'[]','comment':'x'}]}]");
            assertThat(thrown.getMessage(), startsWith("Stage 'SOURCE_LITERAL_LIST' does not declare key 'comment'"));
        }

        @Test
        @DisplayName("in a configuration fails fromConfig naming the stage and the key")
        void inConfigFails() {
            StageConfig cfg = StageConfig.builder().string("regex", ",").string("limit", "2").build();
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> StageReflection.of(SplitTransform.class).fromConfig(cfg));
            assertThat(thrown.getMessage(), startsWith("Stage 'TRANSFORM_SPLIT' does not declare key 'limit'"));
        }

    }

    @Nested
    @DisplayName("A JSON null on an optional key")
    class NullOptionalKey {

        @Test
        @DisplayName("of a scalar slot loads as if the key were absent")
        void scalarReadsAsAbsent() {
            DataPipeline<?> pipeline = PipelineGson.fromJson(q(
                "[{'kind':'SOURCE_URL','outputType':'RAW_HTML','url':'https://example.com','maxBodyBytes':null}]"
            ));
            assertThat(((UrlSource) pipeline.stages().getFirst()).maxBodyBytes(), is(nullValue()));
        }

        @Test
        @DisplayName("of a sub-pipeline slot loads as if the key were absent")
        void subPipelineReadsAsAbsent() {
            DataPipeline<?> pipeline = PipelineGson.fromJson(q("[" + SOURCE + ",{'kind':'TRANSFORM_COALESCE','inputType':'STRING',"
                + "'outputType':'STRING','body':[{'kind':'TRANSFORM_TRIM'}],'fallback':null,'defaultValue':'x','when':null}]"));
            assertThat(((CoalesceTransform<?, ?>) pipeline.stages().getLast()).fallback(), is(nullValue()));
        }

        @Test
        @DisplayName("is not written back")
        void notWrittenBack() {
            String json = PipelineGson.toJson(PipelineGson.fromJson(q(
                "[{'kind':'SOURCE_URL','outputType':'RAW_HTML','url':'https://example.com','maxBodyBytes':null}]"
            )));
            assertThat(json, not(containsString("maxBodyBytes")));
        }

    }

    @Nested
    @DisplayName("A factory refusal")
    class FactoryRefusal {

        @Test
        @DisplayName("fails the load as the factory's own IllegalArgumentException")
        void reachesLoaderUnwrapped() {
            IllegalArgumentException thrown = loadFails("[{'kind':'SOURCE_URL','outputType':'RAW_HTML','url':'u','maxBodyBytes':-1}]");
            assertThat(thrown.getMessage(), is(equalTo("UrlSource maxBodyBytes must not be negative but got '-1'")));
        }

        @Test
        @DisplayName("inside a body fails the load as the factory's own IllegalArgumentException")
        void inBodyReachesLoaderUnwrapped() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_MAP','elementInputType':'STRING',"
                + "'elementOutputType':'INT','body':[{'kind':'TRANSFORM_TRIM'}]}]");
            assertThat(thrown.getMessage(), startsWith("Invalid map body"));
        }

        @Test
        @DisplayName("fails fromConfig as the factory's own IllegalArgumentException")
        void reachesFromConfigUnwrapped() {
            StageConfig cfg = StageConfig.builder()
                .dataType("outputType", DataTypes.INT)
                .string("url", "u")
                .build();
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> StageReflection.of(UrlSource.class).fromConfig(cfg));
            assertThat(thrown.getMessage(), startsWith("UrlSource supports "));
        }

    }

    @Nested
    @DisplayName("A stage entry")
    class StageEntry {

        @Test
        @DisplayName("without a kind fails the load")
        void withoutKindFails() {
            IllegalArgumentException thrown = loadFails("[{'outputType':'STRING','value':'a'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage entry is missing required key 'kind'")));
        }

        @Test
        @DisplayName("whose kind is a JSON null fails the load")
        void nullKindFails() {
            IllegalArgumentException thrown = loadFails("[{'kind':null,'outputType':'STRING','value':'a'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage entry is missing required key 'kind'")));
        }

        @Test
        @DisplayName("whose kind is not a stage id fails the load")
        void objectKindFails() {
            IllegalArgumentException thrown = loadFails("[{'kind':{},'outputType':'STRING','value':'a'}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage entry holds a JSON object under 'kind' but a stage id was expected")));
        }

        @Test
        @DisplayName("that is not a JSON object fails the load")
        void notAnObjectFails() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",'TRANSFORM_TRIM']");
            assertThat(thrown.getMessage(), is(equalTo("Stage entry must be a JSON object but was a JSON primitive")));
        }

        @Test
        @DisplayName("that is not a JSON object inside a body fails the load")
        void notAnObjectInBodyFails() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_MAP','elementInputType':'STRING',"
                + "'elementOutputType':'STRING','body':[null]}]");
            assertThat(thrown.getMessage(), is(equalTo("Stage entry must be a JSON object but was a JSON null")));
        }

    }

    @Nested
    @DisplayName("A slot value of the wrong JSON shape")
    class SlotShape {

        @Test
        @DisplayName("on a scalar slot fails the load naming the key")
        void scalarHoldingObjectFails() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'TRANSFORM_SPLIT','regex':{}}]");
            assertThat(thrown.getMessage(), is(equalTo("Field 'regex' holds a JSON object but a STRING was expected")));
        }

        @Test
        @DisplayName("on a sub-pipeline slot fails the load naming the key")
        void subPipelineHoldingObjectFails() {
            IllegalArgumentException thrown = loadFails("[" + LIST_SOURCE + ",{'kind':'TRANSFORM_MAP','elementInputType':'STRING',"
                + "'elementOutputType':'STRING','body':{}}]");
            assertThat(thrown.getMessage(), is(equalTo("Field 'body' holds a JSON object but a SUB_PIPELINE was expected")));
        }

        @Test
        @DisplayName("on a named sub-pipeline fails the load naming the branch")
        void namedBranchHoldingObjectFails() {
            IllegalArgumentException thrown = loadFails("[" + SOURCE + ",{'kind':'COLLECT_MAP','inputType':'STRING',"
                + "'outputs':{'n':{}}}]");
            assertThat(thrown.getMessage(), is(equalTo("Sub-pipeline 'n' must be a stage array but was a JSON object")));
        }

    }

    @Nested
    @DisplayName("A typed output entry")
    class TypedOutput {

        private static @NotNull String objectBuild(@NotNull String entry) {
            return "[" + SOURCE + ",{'kind':'TRANSFORM_JSON_OBJECT_BUILD','inputType':'STRING','outputs':{'n':" + entry + "}}]";
        }

        @Test
        @DisplayName("without an outputType fails the load naming the output and the key")
        void withoutOutputTypeFails() {
            IllegalArgumentException thrown = loadFails(objectBuild("{'chain':[{'kind':'TRANSFORM_STRING_LENGTH'}]}"));
            assertThat(thrown.getMessage(), is(equalTo("Typed sub-pipeline 'n' is missing required key 'outputType'")));
        }

        @Test
        @DisplayName("whose chain is a JSON null fails the load as a missing key")
        void nullChainFails() {
            IllegalArgumentException thrown = loadFails(objectBuild("{'outputType':'INT','chain':null}"));
            assertThat(thrown.getMessage(), is(equalTo("Typed sub-pipeline 'n' is missing required key 'chain'")));
        }

        @Test
        @DisplayName("with an undeclared key fails the load naming the output and the key")
        void undeclaredKeyFails() {
            IllegalArgumentException thrown = loadFails(objectBuild("{'outputType':'INT','chain':[{'kind':'TRANSFORM_STRING_LENGTH'}],'type':'INT'}"));
            assertThat(thrown.getMessage(), is(equalTo(
                "Typed sub-pipeline 'n' does not declare key 'type' (declared keys: [outputType, chain])"
            )));
        }

        @Test
        @DisplayName("that is not a JSON object fails the load naming the output")
        void notAnObjectFails() {
            IllegalArgumentException thrown = loadFails(objectBuild("[]"));
            assertThat(thrown.getMessage(), is(equalTo("Typed sub-pipeline 'n' must be a JSON object but was a JSON array")));
        }

    }

}
