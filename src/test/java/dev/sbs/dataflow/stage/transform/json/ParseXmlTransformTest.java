package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.json.ParseXmlTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class ParseXmlTransformTest {

    private final ParseXmlTransform stage = ParseXmlTransform.of();

    @Test
    @DisplayName("Sample XML fixture parses into a JsonElement preserving nested fields")
    void parsesNestedFixture() {
        JsonElement root = stage.execute(PipelineContext.empty(), Fixtures.load("sample.xml"));
        assertThat(root.isJsonObject(), is(true));
        // XmlMapper -> JsonElement: each child element becomes a property; values are strings.
        assertThat(root.getAsJsonObject().get("name").getAsString(), is(equalTo("Dark Claymore")));
        JsonElement stats = root.getAsJsonObject().get("stats");
        assertThat(stats.getAsJsonObject().get("dmg").getAsString(), is(equalTo("500")));
    }

}
