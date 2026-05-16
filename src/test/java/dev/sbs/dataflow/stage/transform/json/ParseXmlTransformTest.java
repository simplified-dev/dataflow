package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParseXmlTransformTest {

    private final ParseXmlTransform stage = ParseXmlTransform.of();
    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Sample XML fixture parses into a JsonElement preserving nested fields")
    void parsesNestedFixture() {
        JsonElement root = stage.execute(this.ctx, Fixtures.load("sample.xml"));
        assertThat(root.isJsonObject(), is(true));
        assertThat(root.getAsJsonObject().get("name").getAsString(), is(equalTo("Dark Claymore")));
        JsonElement stats = root.getAsJsonObject().get("stats");
        assertThat(stats.getAsJsonObject().get("dmg").getAsString(), is(equalTo("500")));
    }

    @Test
    @DisplayName("Repeated sibling elements with the same name parse into a JsonArray under that key")
    void listShapedChildren() {
        JsonElement root = stage.execute(this.ctx, "<root><item>a</item><item>b</item><item>c</item></root>");
        JsonElement items = root.getAsJsonObject().get("item");
        assertThat(items.isJsonArray(), is(true));
        assertThat(items.getAsJsonArray().size(), is(equalTo(3)));
        assertThat(items.getAsJsonArray().get(0).getAsString(), is(equalTo("a")));
        assertThat(items.getAsJsonArray().get(2).getAsString(), is(equalTo("c")));
    }

    @Test
    @DisplayName("Mixed-content text on an attributed element lands under TEXT_KEY")
    void mixedContentText() {
        JsonElement root = stage.execute(this.ctx, "<root><elem attr=\"x\">hello</elem></root>");
        JsonElement elem = root.getAsJsonObject().get("elem");
        assertThat(elem.getAsJsonObject().get("attr").getAsString(), is(equalTo("x")));
        assertThat(elem.getAsJsonObject().get(ParseXmlTransform.TEXT_KEY).getAsString(), is(equalTo("hello")));
    }

    @Test
    @DisplayName("Numeric-looking and boolean-looking element text stays a string (XML has no native types)")
    void primitiveLookingTextStaysString() {
        JsonElement root = stage.execute(this.ctx, "<root><n>42</n><b>true</b></root>");
        JsonElement n = root.getAsJsonObject().get("n");
        JsonElement b = root.getAsJsonObject().get("b");
        assertThat(n.getAsJsonPrimitive().isString(), is(true));
        assertThat(n.getAsString(), is(equalTo("42")));
        assertThat(b.getAsJsonPrimitive().isString(), is(true));
        assertThat(b.getAsString(), is(equalTo("true")));
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Malformed XML throws IllegalArgumentException")
    void malformedXml() {
        assertThrows(
            IllegalArgumentException.class,
            () -> stage.execute(this.ctx, "<unclosed>")
        );
    }

}
