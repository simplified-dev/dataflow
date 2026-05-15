package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.string.PrefixTransform;
import dev.sbs.dataflow.stage.transform.string.UpperCaseTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonObjectBuildTransformTest {

    @Test
    @DisplayName("Builds a JsonObject from named typed branches")
    void buildsObjectFromBranches() {
        JsonObjectBuildTransform<String> stage = JsonObjectBuildTransform.over(DataTypes.STRING)
            .output("upper", DataTypes.STRING, chain -> chain
                .stage(UpperCaseTransform.of()))
            .output("prefixed", DataTypes.STRING, chain -> chain
                .stage(PrefixTransform.of(">>>")))
            .build();

        JsonObject result = stage.execute(PipelineContext.defaults(), "hello");

        assertThat(result, is(notNullValue()));
        assertThat(result.get("upper").getAsString(), is(equalTo("HELLO")));
        assertThat(result.get("prefixed").getAsString(), is(equalTo(">>>hello")));
    }

    @Test
    @DisplayName("Null input returns null without evaluating branches")
    void nullInputReturnsNull() {
        JsonObjectBuildTransform<String> stage = JsonObjectBuildTransform.over(DataTypes.STRING)
            .output("x", DataTypes.STRING, chain -> chain.stage(UpperCaseTransform.of()))
            .build();

        JsonObject result = stage.execute(PipelineContext.defaults(), null);
        assertThat(result, is(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    @DisplayName("Rejects branch bodies whose type chain does not match the declared output")
    void rejectsTypeMismatch() {
        // UpperCaseTransform produces STRING - declaring INT triggers chain validation failure.
        assertThrows(IllegalArgumentException.class, () ->
            JsonObjectBuildTransform.over(DataTypes.STRING)
                .output("bad", DataTypes.INT, chain -> chain.stage(UpperCaseTransform.of()))
                .build());
    }

    @Test
    @DisplayName("Empty branch body is rejected (StageChainValidator requires at least one stage)")
    void emptyBranchInvalidWhenTypesDiffer() {
        // Body must produce INT but is empty; empty body produces input type STRING.
        assertThat(
            assertThrows(IllegalArgumentException.class, () ->
                JsonObjectBuildTransform.over(DataTypes.STRING)
                    .output("bad", DataTypes.INT, chain -> { })
                    .build()
            ).getMessage(),
            org.hamcrest.Matchers.containsString("no stages")
        );
    }

    @Test
    @DisplayName("Output type is JSON_OBJECT; advertises COLLECT_MAP-style kind")
    void advertisesContract() {
        JsonObjectBuildTransform<String> stage = JsonObjectBuildTransform.over(DataTypes.STRING).build();
        assertThat(stage.outputType(), is(DataTypes.JSON_OBJECT));
        assertThat(stage.kind().name(), is(equalTo("TRANSFORM_JSON_OBJECT_BUILD")));
    }

}
