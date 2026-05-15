package dev.sbs.dataflow;

import dev.sbs.dataflow.serde.PipelineGson;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.LiteralSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.predicate.string.StringContainsPredicate;
import dev.sbs.dataflow.stage.terminal.collect.JsonObjectFromEntriesCollect;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.json.GsonDeserializeTransform;
import dev.sbs.dataflow.stage.transform.json.JsonObjectBuildTransform;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class WikiPipelineE2ETest {

    record DarkClaymore(
        String type,
        String rarity,
        Map<String, Integer> stats,
        Map<String, Boolean> properties
    ) {}

    private static final DataType<DarkClaymore> DARK_CLAYMORE_TYPE =
        new DataType.Basic<>(DarkClaymore.class, "DARK_CLAYMORE");

    @BeforeAll
    static void registerType() {
        DataTypes.register(DARK_CLAYMORE_TYPE);
    }

    @Test
    @DisplayName("Full chain - Paste(HTML) -> ParseHtml -> CssSelect -> Filter Dmg -> First -> Nth -> Text -> Regex -> ParseInt - yields 500")
    void darkClaymoreDmgIsFiveHundred() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.rawHtml(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(DomTextContainsFilter.of("Dmg"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NthChildTransform.of("td", 1))
            .stage(NodeTextTransform.of())
            .stage(RegexExtractTransform.of("\\d+"))
            .stage(ParseIntTransform.of())
            .build();

        assertThat(pipeline.validate().isValid(), is(true));
        Object result = pipeline.execute(PipelineContext.empty());
        assertThat(result, is(equalTo(500)));
    }

    @Test
    @DisplayName("Generic execute<T> infers the result type at the call site")
    void executeInfersResultType() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.rawHtml(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(DomTextContainsFilter.of("Dmg"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NthChildTransform.of("td", 1))
            .stage(NodeTextTransform.of())
            .stage(RegexExtractTransform.of("\\d+"))
            .stage(ParseIntTransform.of())
            .build();

        // T is inferred from the assignment - no explicit cast needed.
        Integer dmg = pipeline.execute(PipelineContext.empty());
        assertThat(dmg, is(equalTo(500)));
    }

    @Test
    @DisplayName("Full infobox -> JsonObject -> Gson-deserialised DarkClaymore record")
    void darkClaymoreDeserialisesToRecord() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.rawHtml(Fixtures.load("dark_claymore_full.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("div.infobox"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(buildClaymoreObject())
            .stage(GsonDeserializeTransform.of(DataTypes.JSON_OBJECT, DARK_CLAYMORE_TYPE))
            .build();

        DarkClaymore result = pipeline.execute(PipelineContext.empty());

        assertThat(result, is(notNullValue()));
        assertThat(result.type(), is(equalTo("Longsword")));
        assertThat(result.rarity(), is(equalTo("LEGENDARY")));
        assertThat(result.stats(), hasEntry(equalTo("Dmg"), equalTo(500)));
        assertThat(result.stats(), hasEntry(equalTo("Str"), equalTo(100)));
        assertThat(result.stats(), hasEntry(equalTo("Cr Dmg"), equalTo(100)));
        assertThat(result.stats(), hasEntry(equalTo("Swing"), equalTo(2)));
        assertThat(result.properties(), hasEntry(equalTo("Enchantable"), equalTo(true)));
        assertThat(result.properties(), hasEntry(equalTo("Reforgeable"), equalTo(true)));
        assertThat(result.properties(), hasEntry(equalTo("Salable"), equalTo(false)));
        assertThat(result.properties(), hasEntry(equalTo("Tradeable"), equalTo(true)));
    }

    @Test
    @DisplayName("Live wiki pipeline round-trips through PipelineGson and re-executes equivalently")
    @EnabledIfEnvironmentVariable(named = "DATAFLOW_LIVE", matches = "true")
    void darkClaymoreLivePipelineSurvivesReserialisation() {
        DataPipeline original = DataPipeline.builder()
            .source(UrlSource.rawHtml("https://hypixelskyblock.minecraft.wiki/w/Dark_Claymore"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("div.infobox"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(buildClaymoreObject())
            .stage(GsonDeserializeTransform.of(DataTypes.JSON_OBJECT, DARK_CLAYMORE_TYPE))
            .build();

        DarkClaymore firstResult = original.execute(PipelineContext.empty());
        assertThat("Live wiki returned an empty infobox (likely bot challenge); set DATAFLOW_LIVE=false or run locally",
            firstResult, is(notNullValue()));

        String json = PipelineGson.toJson(original);
        DataPipeline rebuilt = PipelineGson.fromJson(json);
        DarkClaymore secondResult = rebuilt.execute(PipelineContext.empty());

        assertThat(secondResult, is(equalTo(firstResult)));
    }

    /**
     * Builds the per-claymore JsonObject from a single {@code <div class="infobox">} element.
     * Reused by both the fixture-based and live-URL tests.
     */
    private static JsonObjectBuildTransform<org.jsoup.nodes.Element> buildClaymoreObject() {
        return JsonObjectBuildTransform.over(DataTypes.DOM_NODE)
            .output("type", DataTypes.STRING, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-label:matchesOwn(^Type$) + div.infobox-row-value"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(TrimTransform.of()))
            .output("rarity", DataTypes.STRING, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-label:matchesOwn(^Rarity$) + div.infobox-row-value"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(TrimTransform.of()))
            .output("stats", DataTypes.JSON_OBJECT, chain -> chain
                .stage(CssSelectTransform.of(
                    "div.group:has(> div.infobox-header:matchesOwn(^Stats$)) > div.infobox-row-container"))
                .stage(MapTransform.of(DataTypes.DOM_NODE, DataTypes.JSON_OBJECT, List.of(
                    statsEntryBuilder())))
                .stage(JsonObjectFromEntriesCollect.of()))
            .output("properties", DataTypes.JSON_OBJECT, chain -> chain
                .stage(CssSelectTransform.of(
                    "div.group:has(> div.infobox-header:matchesOwn(^Properties$)) > div.infobox-row-container"))
                .stage(MapTransform.of(DataTypes.DOM_NODE, DataTypes.JSON_OBJECT, List.of(
                    propertiesEntryBuilder())))
                .stage(JsonObjectFromEntriesCollect.of()))
            .build();
    }

    private static JsonObjectBuildTransform<org.jsoup.nodes.Element> statsEntryBuilder() {
        return JsonObjectBuildTransform.over(DataTypes.DOM_NODE)
            .output("key", DataTypes.STRING, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-label"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(ReplaceTransform.of("[^A-Za-z ]", ""))
                .stage(TrimTransform.of())
                .stage(ReplaceTransform.of("\\s+", " ")))
            .output("value", DataTypes.INT, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-value"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("-?\\d+"))
                .stage(ParseIntTransform.of()))
            .build();
    }

    private static JsonObjectBuildTransform<org.jsoup.nodes.Element> propertiesEntryBuilder() {
        return JsonObjectBuildTransform.over(DataTypes.DOM_NODE)
            .output("key", DataTypes.STRING, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-label"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(TrimTransform.of()))
            .output("value", DataTypes.BOOLEAN, chain -> chain
                .stage(CssSelectTransform.of("div.infobox-row-value"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NodeTextTransform.of())
                .stage(StringContainsPredicate.of("Yes")))
            .build();
    }

}
