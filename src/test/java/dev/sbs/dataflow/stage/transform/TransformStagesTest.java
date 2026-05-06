package dev.sbs.dataflow.stage.transform;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.PipelineContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class TransformStagesTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("CssSelect returns every matching descendant element")
    void cssSelect() {
        Element root = Jsoup.parse("<div><p>a</p><p>b</p></div>");
        List<Element> matches = CssSelectTransform.of("p").execute(this.ctx, root);
        assertThat(matches, is(notNullValue()));
        assertThat(matches.size(), is(equalTo(2)));
        assertThat(matches.get(0).text(), is(equalTo("a")));
    }

    @Test
    @DisplayName("NodeText returns the visible text")
    void nodeText() {
        Element root = Jsoup.parse("<p>hello <b>world</b></p>").selectFirst("p");
        assertThat(NodeTextTransform.create().execute(this.ctx, root), is(equalTo("hello world")));
    }

    @Test
    @DisplayName("NodeAttr returns the attribute value")
    void nodeAttr() {
        Element link = Jsoup.parse("<a href='https://example.com'>x</a>").selectFirst("a");
        assertThat(NodeAttrTransform.of("href").execute(this.ctx, link), is(equalTo("https://example.com")));
    }

    @Test
    @DisplayName("NthChild picks the requested child by selector + index")
    void nthChild() {
        Element row = Jsoup.parse("<table><tr><td>a</td><td>b</td><td>c</td></tr></table>").selectFirst("tr");
        Element second = NthChildTransform.of("td", 1).execute(this.ctx, row);
        assertThat(second, is(notNullValue()));
        assertThat(second.text(), is(equalTo("b")));
    }

    @Test
    @DisplayName("NthChild returns null when index is out of range")
    void nthChildOutOfRange() {
        Element row = Jsoup.parse("<table><tr><td>a</td></tr></table>").selectFirst("tr");
        assertThat(NthChildTransform.of("td", 5).execute(this.ctx, row), is(nullValue()));
    }

    @Test
    @DisplayName("JsonPath walks dot-separated keys through a JsonObject tree")
    void jsonPath() {
        JsonElement root = JsonParser.parseString("{\"a\":{\"b\":{\"c\":42}}}");
        JsonElement leaf = JsonPathTransform.of("a.b.c").execute(this.ctx, root);
        assertThat(leaf, is(notNullValue()));
        assertThat(leaf.getAsInt(), is(equalTo(42)));
    }

    @Test
    @DisplayName("JsonPath returns null on missing segment")
    void jsonPathMissingSegment() {
        JsonElement root = JsonParser.parseString("{\"a\":{}}");
        assertThat(JsonPathTransform.of("a.b.c").execute(this.ctx, root), is(nullValue()));
    }

    @Test
    @DisplayName("JsonField extracts a single field of a JsonObject")
    void jsonField() {
        com.google.gson.JsonObject obj = JsonParser.parseString("{\"x\":\"y\"}").getAsJsonObject();
        JsonElement value = JsonFieldTransform.of("x").execute(this.ctx, obj);
        assertThat(value, is(notNullValue()));
        assertThat(value.getAsString(), is(equalTo("y")));
    }

    @Test
    @DisplayName("RegexExtract returns the first match by default")
    void regexExtract() {
        assertThat(RegexExtractTransform.of("\\d+").execute(this.ctx, "ab 500 cd"), is(equalTo("500")));
    }

    @Test
    @DisplayName("RegexExtract returns the requested capture group")
    void regexExtractGroup() {
        String result = RegexExtractTransform.of("(\\d+)\\s*Dmg", 1).execute(this.ctx, "500 Dmg");
        assertThat(result, is(equalTo("500")));
    }

    @Test
    @DisplayName("RegexExtract returns null when there is no match")
    void regexExtractNoMatch() {
        assertThat(RegexExtractTransform.of("\\d+").execute(this.ctx, "no digits here"), is(nullValue()));
    }

    @Test
    @DisplayName("ParseInt handles whitespace and invalid input")
    void parseInt() {
        assertThat(ParseIntTransform.create().execute(this.ctx, "  42  "), is(equalTo(42)));
        assertThat(ParseIntTransform.create().execute(this.ctx, "nope"), is(nullValue()));
    }

    @Test
    @DisplayName("ParseDouble handles whitespace and invalid input")
    void parseDouble() {
        assertThat(ParseDoubleTransform.create().execute(this.ctx, " 3.14 "), is(equalTo(3.14)));
        assertThat(ParseDoubleTransform.create().execute(this.ctx, "x"), is(nullValue()));
    }

    @Test
    @DisplayName("Trim strips surrounding whitespace")
    void trim() {
        assertThat(TrimTransform.create().execute(this.ctx, "  hi  "), is(equalTo("hi")));
    }

    @Test
    @DisplayName("Replace runs the regex over the input")
    void replace() {
        String result = ReplaceTransform.of("\\s+", "-").execute(this.ctx, "hello world  there");
        assertThat(result, is(equalTo("hello-world-there")));
    }

    @Test
    @DisplayName("Split breaks the string on the regex into a list")
    void split() {
        List<String> parts = SplitTransform.of(",").execute(this.ctx, "a,b,c");
        assertThat(parts, contains("a", "b", "c"));
    }

    @Test
    @DisplayName("Wiki partial chain: parse fixture, css select rows, filter Dmg row")
    void wikiPartialChain() {
        Element root = Jsoup.parse(dev.sbs.dataflow.Fixtures.load("dark_claymore.html"));
        Elements rows = root.select("table.infobox tr");
        assertThat(rows.size(), is(greaterThan(5)));

        // Run the actual stages to exercise the chain
        List<Element> all = CssSelectTransform.of("table.infobox tr").execute(this.ctx, root);
        assertThat(all, is(notNullValue()));
        List<Element> filtered = dev.sbs.dataflow.stage.filter.DomTextContainsFilter.of("Dmg")
            .execute(this.ctx, all);
        assertThat(filtered, is(notNullValue()));
        assertThat(filtered.size(), is(equalTo(1)));
        assertThat(filtered.get(0).text(), is(equalTo("Dmg 500")));
    }

}
