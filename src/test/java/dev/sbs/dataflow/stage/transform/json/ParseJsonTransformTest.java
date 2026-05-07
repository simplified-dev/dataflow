package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.json.ParseJsonTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class ParseJsonTransformTest {

    private final ParseJsonTransform stage = ParseJsonTransform.of();

    @Test
    @DisplayName("Sample JSON fixture parses and exposes nested fields")
    void parsesNestedFixture() {
        JsonElement root = stage.execute(PipelineContext.empty(), Fixtures.load("sample.json"));
        assertThat(root.isJsonObject(), is(true));
        assertThat(root.getAsJsonObject().get("name").getAsString(), is(equalTo("Dark Claymore")));
        JsonElement stats = root.getAsJsonObject().get("stats");
        assertThat(stats.getAsJsonObject().get("dmg").getAsInt(), is(equalTo(500)));
    }

}
