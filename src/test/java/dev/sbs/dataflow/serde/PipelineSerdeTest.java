package dev.sbs.dataflow.serde;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.FieldType;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageMetadata;
import dev.sbs.dataflow.stage.StageReflection;
import dev.sbs.dataflow.stage.StageRegistry;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.terminal.collect.MapCollect;
import dev.sbs.dataflow.stage.terminal.collect.JoinCollect;
import dev.sbs.dataflow.stage.terminal.collect.LastCollect;
import dev.sbs.dataflow.stage.terminal.collect.ListCollect;
import dev.sbs.dataflow.stage.terminal.collect.SetCollect;
import dev.sbs.dataflow.stage.source.EmbedSource;
import dev.sbs.dataflow.stage.filter.list.DistinctFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.LiteralSource;
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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class PipelineSerdeTest {

    @Test
    @DisplayName("Round-trip the wiki pipeline produces identical execution result")
    void roundTripWikiPipeline() {
        DataPipeline original = DataPipeline.builder()
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

        String json = PipelineGson.toJson(original);
        DataPipeline rebuilt = PipelineGson.fromJson(json);

        assertThat(rebuilt.validate().isValid(), is(true));
        assertThat(rebuilt.execute(), is(equalTo(500)));
    }

    @Test
    @DisplayName("Round-trip JSON is stable - serialise -> deserialise -> serialise yields the same JSON")
    void serdeIsIdempotent() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "hi"))
            .stage(TrimTransform.of())
            .build();
        String first = PipelineGson.toJson(pipeline);
        String second = PipelineGson.toJson(PipelineGson.fromJson(first));
        assertThat(second, is(equalTo(first)));
    }

    @Test
    @DisplayName("Each registered stage round-trips through JSON and re-executes equivalently")
    void everyRegisteredStageRoundTrips() {
        // Build a synthetic JSON fixture pipeline that exercises Json* stages.
        DataPipeline jsonPipeline = DataPipeline.builder()
            .source(LiteralSource.rawJson(Fixtures.load("sample.json")))
            .stage(ParseJsonTransform.of())
            .stage(JsonPathTransform.of("stats.dmg"))
            .build();
        roundTrip(jsonPipeline);

        DataPipeline xmlPipeline = DataPipeline.builder()
            .source(LiteralSource.rawXml(Fixtures.load("sample.xml")))
            .stage(ParseXmlTransform.of())
            .stage(JsonPathTransform.of("name"))
            .build();
        roundTrip(xmlPipeline);

        DataPipeline urlPipeline = DataPipeline.builder()
            .source(UrlSource.rawHtml("http://example.com/x"))
            .build();
        roundTrip(urlPipeline);

        DataPipeline allTransforms = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "  hello WORLD  "))
            .stage(TrimTransform.of())
            .stage(ReplaceTransform.of("WORLD", "world"))
            .stage(RegexExtractTransform.of("\\w+", 0))
            .stage(SplitTransform.of(""))
            .build();
        roundTrip(allTransforms);

        DataPipeline collectVariants = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "a,b,a,c"))
            .stage(SplitTransform.of(","))
            .stage(DistinctFilter.of(DataTypes.STRING))
            .stage(JoinCollect.of("|"))
            .build();
        roundTrip(collectVariants);

        DataPipeline collectFlavours = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "x"))
            .stage(SplitTransform.of(""))
            .stage(LastCollect.of(DataTypes.STRING))
            .build();
        roundTrip(collectFlavours);

        // attribute, json field, parseDouble, set
        DataPipeline misc1 = DataPipeline.builder()
            .source(LiteralSource.rawHtml("<a href='https://x'>z</a>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("a"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NodeAttrTransform.of("href"))
            .build();
        roundTrip(misc1);

        DataPipeline misc2 = DataPipeline.builder()
            .source(LiteralSource.rawJson("{\"x\": 3.14}"))
            .stage(ParseJsonTransform.of())
            .stage(JsonPathTransform.of("x"))
            .build();
        roundTrip(misc2);

        DataPipeline misc3 = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "3.14"))
            .stage(ParseDoubleTransform.of())
            .build();
        roundTrip(misc3);

        DataPipeline setVariant = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "a,b,a,c"))
            .stage(SplitTransform.of(","))
            .stage(SetCollect.of(DataTypes.STRING))
            .build();
        roundTrip(setVariant);

        DataPipeline listVariant = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "a,b,c"))
            .stage(SplitTransform.of(","))
            .stage(ListCollect.of(DataTypes.STRING))
            .build();
        roundTrip(listVariant);

    }

    @Test
    @DisplayName("MapCollect with three outputs round-trips and re-executes to identical map")
    void mapCollectRoundTrips() {
        DataType<List<org.jsoup.nodes.Element>> input = DataType.list(DataTypes.DOM_NODE);
        MapCollect<List<org.jsoup.nodes.Element>> collect = MapCollect.over(input)
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
            .source(LiteralSource.rawHtml(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(collect)
            .build();

        String json = PipelineGson.toJson(original);
        DataPipeline rebuilt = PipelineGson.fromJson(json);
        Object result = rebuilt.execute();

        assertThat(result, is(equalTo(java.util.Map.of("dmg", 500, "strength", 220))));
    }

    @Test
    @DisplayName("EmbedSource round-trips")
    void embedRoundTrips() {
        DataPipeline outer = DataPipeline.builder()
            .source(EmbedSource.of("saved-id", DataTypes.STRING))
            .build();
        String json = PipelineGson.toJson(outer);
        assertThat(json, containsString("\"embeddedPipelineId\":\"saved-id\""));

        DataPipeline rebuilt = PipelineGson.fromJson(json);
        Stage<?, ?> first = rebuilt.stages().getFirst();
        assertThat(first.kindId(), is(equalTo("SOURCE_EMBED")));
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
    @DisplayName("Unknown stage id is rejected with a clear error")
    void unknownKindRejected() {
        try {
            PipelineGson.fromJson("[{\"kind\":\"NOT_A_REAL_KIND\"}]");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("NOT_A_REAL_KIND"));
            return;
        }
        throw new AssertionError("Expected IllegalArgumentException for unknown kind");
    }

    /**
     * Per-stage smoke test: every {@link StageRegistry registered stage} with a non-chain schema
     * must instantiate cleanly from its {@link FieldSpec#placeholder()} defaults, and the resulting
     * stage's {@code config()} must round-trip through the factory back to an equivalent stage.
     * Chain-bearing stages are excluded because their bodies have no schema-level default.
     */
    @TestFactory
    Stream<DynamicTest> everyNonChainKindFactoryRoundTrips() {
        Set<FieldType> chainFieldTypes = EnumSet.of(
            FieldType.SUB_PIPELINE,
            FieldType.SUB_PIPELINES_MAP,
            FieldType.TYPED_SUB_PIPELINES_MAP
        );

        return StageRegistry.allOrdered().stream()
            .filter(cls -> StageReflection.of(cls).schema().stream().noneMatch(s -> chainFieldTypes.contains(s.type())))
            .map(cls -> {
                StageMetadata metadata = StageReflection.of(cls);
                String id = metadata.annotation().id();
                return DynamicTest.dynamicTest(id, () -> {
                    StageConfig cfg = buildDefaultConfig(cls, metadata);
                    Stage<?, ?> stage = metadata.fromConfig(cfg);
                    assertThat(stage, is(notNullValue()));
                    assertThat(stage.kindId(), is(equalTo(id)));
                    // Round-trip the stage's own config back through the factory.
                    Stage<?, ?> rebuilt = metadata.fromConfig(stage.config());
                    assertThat(rebuilt.kindId(), is(equalTo(id)));
                    assertThat(rebuilt.inputType(), is(equalTo(stage.inputType())));
                    assertThat(rebuilt.outputType(), is(equalTo(stage.outputType())));
                });
            });
    }

    private static StageConfig buildDefaultConfig(Class<? extends Stage<?, ?>> cls, StageMetadata metadata) {
        StageSpec spec0 = metadata.annotation();
        StageConfig.Builder b = StageConfig.builder();
        for (FieldSpec<?> spec : metadata.schema()) {
            String placeholder = spec.placeholder();
            switch (spec.type()) {
                case STRING -> b.string(spec.name(), placeholder);
                case INT    -> b.integer(spec.name(), placeholder.isEmpty() ? 0 : Integer.parseInt(placeholder));
                case LONG   -> b.longVal(spec.name(), placeholder.isEmpty() ? 0L : Long.parseLong(placeholder));
                case DOUBLE -> b.doubleVal(spec.name(), placeholder.isEmpty() ? 0d : Double.parseDouble(placeholder));
                case BOOLEAN -> b.bool(spec.name(), Boolean.parseBoolean(placeholder));
                case DATA_TYPE -> {
                    DataType<?> resolved = DataTypes.byLabel(placeholder);
                    if (resolved == null)
                        throw new AssertionError(
                            "Stage " + spec0.id() + " field '" + spec.name() + "' has unknown DataType placeholder '" + placeholder + "'"
                        );
                    b.dataType(spec.name(), resolved);
                }
                default -> {} // chain field types skipped via filter()
            }
        }
        return b.build();
    }

    private static void roundTrip(DataPipeline pipeline) {
        String first = PipelineGson.toJson(pipeline);
        DataPipeline rebuilt = PipelineGson.fromJson(first);
        String second = PipelineGson.toJson(rebuilt);
        assertThat(second, is(equalTo(first)));
    }

}
