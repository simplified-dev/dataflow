package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class MapCollectTest {

    @Test
    @DisplayName("MapCollect fans the same row list out to three named integer outputs")
    void mapCollectProducesNamedOutputs() {
        Element root = Jsoup.parse(Fixtures.load("dark_claymore.html"));
        List<Element> rows = CssSelectTransform.of("table.infobox tr")
            .execute(PipelineContext.empty(), root);
        assertThat(rows, is(notNullValue()));

        DataType<List<Element>> input = DataType.list(DataTypes.DOM_NODE);
        MapCollect<List<Element>> collect = MapCollect.over(input)
            .output("dmg", chain -> chain
                .stage(DomTextContainsFilter.of("Dmg"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .output("strength", chain -> chain
                .stage(DomTextContainsFilter.of("Strength"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .output("crit_damage", chain -> chain
                .stage(DomTextContainsFilter.of("Crit Damage"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(NodeTextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .build();

        Map<String, Object> result = collect.execute(PipelineContext.empty(), rows);

        assertThat(result, hasEntry(equalTo("dmg"), equalTo(500)));
        assertThat(result, hasEntry(equalTo("strength"), equalTo(220)));
        assertThat(result, hasEntry(equalTo("crit_damage"), equalTo(175)));
    }

    @Test
    @DisplayName("MapCollect outputType is MAP_OUTPUT and kind is COLLECT_MAP")
    void mapCollectAdvertisesContract() {
        MapCollect<String> collect = MapCollect.over(DataTypes.STRING).build();
        assertThat(collect.outputType(), is(DataTypes.MAP_OUTPUT));
        assertThat(collect.kind().name(), is(equalTo("COLLECT_MAP")));
    }

}
