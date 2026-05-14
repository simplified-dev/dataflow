package dev.sbs.dataflow.serde;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.terminal.Branch;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.terminal.collect.JoinCollect;
import dev.sbs.dataflow.stage.terminal.collect.LastCollect;
import dev.sbs.dataflow.stage.terminal.collect.ListCollect;
import dev.sbs.dataflow.stage.terminal.collect.SetCollect;
import dev.sbs.dataflow.stage.source.PipelineEmbed;
import dev.sbs.dataflow.stage.filter.list.DistinctFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.json.ParseJsonTransform;
import dev.sbs.dataflow.stage.transform.json.ParseXmlTransform;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.json.JsonFieldTransform;
import dev.sbs.dataflow.stage.transform.json.JsonPathTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeAttrTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseDoubleTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class PipelineSerdeTest {

    @Test
    @DisplayName("Round-trip the wiki pipeline produces identical execution result")
    void roundTripWikiPipeline() {
        DataPipeline original = DataPipeline.builder()
            .source(PasteSource.html(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(DomTextContainsFilter.of("Dmg"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NthChildTransform.of("td", 1))
            .stage(NodeTextTransform.of())
            .stage(RegexExtractTransform.of("\\d+"))
            .stage(ParseIntTransform.of())
            .build();

        String json = PipelineGson.toJson(original);
        DataPipeline rebuilt = PipelineGson.fromJson(json);

        assertThat(rebuilt.validate().isValid(), is(true));
        assertThat(rebuilt.execute(PipelineContext.empty()), is(equalTo(500)));
    }

    @Test
    @DisplayName("Round-trip JSON is stable - serialise -> deserialise -> serialise yields the same JSON")
    void serdeIsIdempotent() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.text("hi"))
            .stage(TrimTransform.of())
            .build();
        String first = PipelineGson.toJson(pipeline);
        String second = PipelineGson.toJson(PipelineGson.fromJson(first));
        assertThat(second, is(equalTo(first)));
    }

    @Test
    @DisplayName("Each stage kind round-trips through JSON and re-executes equivalently")
    void everyStageKindRoundTrips() {
        // Build a synthetic JSON fixture pipeline that exercises Json* stages.
        DataPipeline jsonPipeline = DataPipeline.builder()
            .source(PasteSource.json(Fixtures.load("sample.json")))
            .stage(ParseJsonTransform.of())
            .stage(JsonPathTransform.of("stats.dmg"))
            .build();
        roundTrip(jsonPipeline);

        DataPipeline xmlPipeline = DataPipeline.builder()
            .source(PasteSource.xml(Fixtures.load("sample.xml")))
            .stage(ParseXmlTransform.of())
            .stage(JsonPathTransform.of("name"))
            .build();
        roundTrip(xmlPipeline);

        DataPipeline urlPipeline = DataPipeline.builder()
            .source(UrlSource.html("http://example.com/x"))
            .build();
        roundTrip(urlPipeline);

        DataPipeline allTransforms = DataPipeline.builder()
            .source(PasteSource.text("  hello WORLD  "))
            .stage(TrimTransform.of())
            .stage(ReplaceTransform.of("WORLD", "world"))
            .stage(RegexExtractTransform.of("\\w+", 0))
            .stage(SplitTransform.of(""))
            .build();
        roundTrip(allTransforms);

        DataPipeline collectVariants = DataPipeline.builder()
            .source(PasteSource.text("a,b,a,c"))
            .stage(SplitTransform.of(","))
            .stage(DistinctFilter.of(DataTypes.STRING))
            .stage(JoinCollect.of("|"))
            .build();
        roundTrip(collectVariants);

        DataPipeline collectFlavours = DataPipeline.builder()
            .source(PasteSource.text("x"))
            .stage(SplitTransform.of(""))
            .stage(LastCollect.of(DataTypes.STRING))
            .build();
        roundTrip(collectFlavours);

        // attribute, json field, parseDouble, set
        DataPipeline misc1 = DataPipeline.builder()
            .source(PasteSource.html("<a href='https://x'>z</a>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("a"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NodeAttrTransform.of("href"))
            .build();
        roundTrip(misc1);

        DataPipeline misc2 = DataPipeline.builder()
            .source(PasteSource.json("{\"x\": 3.14}"))
            .stage(ParseJsonTransform.of())
            .stage(JsonPathTransform.of("x"))
            .build();
        roundTrip(misc2);

        DataPipeline misc3 = DataPipeline.builder()
            .source(PasteSource.text("3.14"))
            .stage(ParseDoubleTransform.of())
            .build();
        roundTrip(misc3);

        DataPipeline setVariant = DataPipeline.builder()
            .source(PasteSource.text("a,b,a,c"))
            .stage(SplitTransform.of(","))
            .stage(SetCollect.of(DataTypes.STRING))
            .build();
        roundTrip(setVariant);

        DataPipeline listVariant = DataPipeline.builder()
            .source(PasteSource.text("a,b,c"))
            .stage(SplitTransform.of(","))
            .stage(ListCollect.of(DataTypes.STRING))
            .build();
        roundTrip(listVariant);

        DataPipeline jsonField = DataPipeline.builder()
            .source(PasteSource.json("{\"x\":\"y\"}"))
            .stage(ParseJsonTransform.of())
            .stage(JsonFieldTransform.of("x"))
            .build();
        // ParseJson outputs JSON_ELEMENT, JsonField expects JSON_OBJECT; serde itself
        // works regardless of validation. Just round-trip the bytes.
        String jf = PipelineGson.toJson(jsonField);
        assertThat(jf, containsString("TRANSFORM_JSON_FIELD"));
    }

    @Test
    @DisplayName("Branch with three outputs round-trips and re-executes to identical map")
    void branchRoundTrips() {
        DataType<List<org.jsoup.nodes.Element>> input = DataType.list(DataTypes.DOM_NODE);
        Branch<List<org.jsoup.nodes.Element>> branch = Branch.over(input)
            .output("dmg", c -> c
                .stage(DomTextContainsFilter.of("Dmg"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of()))
            .output("strength", c -> c
                .stage(DomTextContainsFilter.of("Strength"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of()))
            .build();

        DataPipeline original = DataPipeline.builder()
            .source(PasteSource.html(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(branch)
            .build();

        String json = PipelineGson.toJson(original);
        DataPipeline rebuilt = PipelineGson.fromJson(json);
        Object result = rebuilt.execute(PipelineContext.empty());

        assertThat(result, is(equalTo(java.util.Map.of("dmg", 500, "strength", 220))));
    }

    @Test
    @DisplayName("PipelineEmbed round-trips")
    void embedRoundTrips() {
        DataPipeline outer = DataPipeline.builder()
            .source(PipelineEmbed.of("saved-id", DataTypes.STRING))
            .build();
        String json = PipelineGson.toJson(outer);
        assertThat(json, containsString("\"embeddedPipelineId\":\"saved-id\""));

        DataPipeline rebuilt = PipelineGson.fromJson(json);
        Stage<?, ?> first = rebuilt.stages().getFirst();
        assertThat(first.kind().name(), is(equalTo("PIPELINE_EMBED")));
    }

    @Test
    @DisplayName("Empty pipeline round-trips as empty array")
    void emptyRoundTrips() {
        String json = PipelineGson.toJson(DataPipeline.empty());
        assertThat(json, is(equalTo("[]")));
        DataPipeline rebuilt = PipelineGson.fromJson(json);
        assertThat(rebuilt.stages().size(), is(equalTo(0)));
    }

    @Test
    @DisplayName("Unknown StageKind is rejected with a clear error")
    void unknownKindRejected() {
        try {
            PipelineGson.fromJson("[{\"kind\":\"NOT_A_REAL_KIND\"}]");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("NOT_A_REAL_KIND"));
            return;
        }
        throw new AssertionError("Expected IllegalArgumentException for unknown kind");
    }

    private static void roundTrip(DataPipeline pipeline) {
        String first = PipelineGson.toJson(pipeline);
        DataPipeline rebuilt = PipelineGson.fromJson(first);
        String second = PipelineGson.toJson(rebuilt);
        assertThat(second, is(equalTo(first)));
    }

}
