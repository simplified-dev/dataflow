package dev.simplified.dataflow.stage.terminal.collect;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.Fixtures;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.filter.dom.TextContainsFilter;
import dev.simplified.dataflow.stage.transform.dom.CssSelectTransform;
import dev.simplified.dataflow.stage.transform.dom.NthChildTransform;
import dev.simplified.dataflow.stage.transform.dom.TextTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.simplified.dataflow.stage.transform.string.RegexExtractTransform;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class MapCollectTest {

    @Test
    @DisplayName("MapCollect fans the same row list out to three named integer outputs")
    void mapCollectProducesNamedOutputs() {
        Element root = Jsoup.parse(Fixtures.load("dark_claymore.html"));
        List<Element> rows = CssSelectTransform.of("table.infobox tr")
            .execute(PipelineContext.defaults(), root);
        assertThat(rows, is(notNullValue()));

        DataType<List<Element>> input = DataType.list(DataTypes.DOM_NODE);
        MapCollect<List<Element>> collect = MapCollect.over(input)
            .output("dmg", chain -> chain
                .stage(TextContainsFilter.of("Dmg"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(TextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .output("strength", chain -> chain
                .stage(TextContainsFilter.of("Strength"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(TextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .output("crit_damage", chain -> chain
                .stage(TextContainsFilter.of("Crit Damage"))
                .stage(FirstCollect.of(DataTypes.DOM_NODE))
                .stage(NthChildTransform.of("td", 1))
                .stage(TextTransform.of())
                .stage(RegexExtractTransform.of("\\d+"))
                .stage(ParseIntTransform.of())
            )
            .build();

        Map<String, Object> result = collect.execute(PipelineContext.defaults(), rows);

        assertThat(result, hasEntry(equalTo("dmg"), equalTo(500)));
        assertThat(result, hasEntry(equalTo("strength"), equalTo(220)));
        assertThat(result, hasEntry(equalTo("crit_damage"), equalTo(175)));
    }

    @Test
    @DisplayName("MapCollect outputType is MAP_OUTPUT and kind is COLLECT_MAP")
    void mapCollectAdvertisesContract() {
        MapCollect<String> collect = MapCollect.over(DataTypes.STRING).build();
        assertThat(collect.outputType(), is(DataTypes.MAP_OUTPUT));
        assertThat(collect.kindId(), is(equalTo("COLLECT_MAP")));
    }

}
