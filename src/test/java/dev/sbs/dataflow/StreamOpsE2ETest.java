package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.terminal.match.AnyMatchCollect;
import dev.sbs.dataflow.stage.terminal.minmax.MaxByCollect;
import dev.sbs.dataflow.stage.terminal.sum.SumIntCollect;
import dev.sbs.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.sbs.dataflow.stage.source.OfListSource;
import dev.sbs.dataflow.stage.source.OfSource;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class StreamOpsE2ETest {

    @Test
    @DisplayName("Source -> ParseHtml -> CssSelect -> MapTransform -> SumIntCollect aggregates DOM values")
    void mapAndSumOverDomRows() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.html(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(MapTransform.of(
                DataTypes.DOM_NODE,
                DataTypes.INT,
                List.of(
                    NthChildTransform.of("td", 1),
                    NodeTextTransform.of(),
                    RegexExtractTransform.of("\\d+"),
                    ParseIntTransform.of()
                )
            ))
            .stage(SumIntCollect.of())
            .build();

        Integer total = pipeline.execute(PipelineContext.empty());
        // Fixture has five numeric infobox rows: Dmg=500, Strength=220, Crit Damage=175,
        // Crit Chance=32, Intelligence=50. Non-numeric rows (Rarity, Type) drop out when
        // RegexExtract(\\d+) returns null. Sum is 500 + 220 + 175 + 32 + 50 = 977.
        assertThat(total, is(977));
    }

    @Test
    @DisplayName("OfListSource feeds a literal int array straight into SumIntCollect")
    void ofListIntoSum() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(OfListSource.of(DataTypes.INT, "[1,2,3,4,5]"))
            .stage(SumIntCollect.of())
            .build();
        assertThat(pipeline.execute(PipelineContext.empty()), is(15));
    }

    @Test
    @DisplayName("OfSource(STRING) -> ParseInt yields the parsed value")
    void ofSourceIntoParse() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(OfSource.of(DataTypes.STRING, "42"))
            .stage(ParseIntTransform.of())
            .build();
        assertThat(pipeline.execute(PipelineContext.empty()), is(42));
    }

    @Test
    @DisplayName("Predicate stages unlock match collectors: OfListSource(INT) -> AnyMatch(body=GreaterThan(12))")
    void predicateUnlocksAnyMatch() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(OfListSource.of(DataTypes.INT, "[1,5,10,15,20]"))
            .stage(AnyMatchCollect.of(DataTypes.INT, List.of(IntGreaterThanPredicate.of(12))))
            .build();
        assertThat(pipeline.execute(PipelineContext.empty()), is(true));
    }

    @Test
    @DisplayName("MaxBy with a key extractor picks the element whose key is largest")
    void maxByPicksLongestString() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(OfListSource.of(DataTypes.STRING, "[\"a\",\"abc\",\"ab\"]"))
            .stage(MaxByCollect.of(DataTypes.STRING, DataTypes.INT, List.of(StringLengthTransform.of())))
            .build();
        assertThat(pipeline.execute(PipelineContext.empty()), is("abc"));
    }

}
