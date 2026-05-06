package dev.sbs.dataflow.stage.branch;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.collect.CollectFirst;
import dev.sbs.dataflow.stage.filter.FilterDomTextContains;
import dev.sbs.dataflow.stage.transform.TransformCssSelect;
import dev.sbs.dataflow.stage.transform.TransformNodeText;
import dev.sbs.dataflow.stage.transform.TransformNthChild;
import dev.sbs.dataflow.stage.transform.TransformParseInt;
import dev.sbs.dataflow.stage.transform.TransformRegexExtract;
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

class BranchTest {

    @Test
    @DisplayName("Branch fans the same row list out to three named integer outputs")
    void branchProducesNamedOutputs() {
        Element root = Jsoup.parse(Fixtures.load("dark_claymore.html"));
        List<Element> rows = TransformCssSelect.of("table.infobox tr")
            .execute(PipelineContext.empty(), root);
        assertThat(rows, is(notNullValue()));

        DataType<List<Element>> input = DataType.list(DataTypes.DOM_NODE);
        Branch<List<Element>> branch = Branch.over(input)
            .output("dmg", chain -> chain
                .stage(FilterDomTextContains.of("Dmg"))
                .stage(CollectFirst.of(DataTypes.DOM_NODE))
                .stage(TransformNthChild.of("td", 1))
                .stage(TransformNodeText.create())
                .stage(TransformRegexExtract.of("\\d+"))
                .stage(TransformParseInt.create())
            )
            .output("strength", chain -> chain
                .stage(FilterDomTextContains.of("Strength"))
                .stage(CollectFirst.of(DataTypes.DOM_NODE))
                .stage(TransformNthChild.of("td", 1))
                .stage(TransformNodeText.create())
                .stage(TransformRegexExtract.of("\\d+"))
                .stage(TransformParseInt.create())
            )
            .output("crit_damage", chain -> chain
                .stage(FilterDomTextContains.of("Crit Damage"))
                .stage(CollectFirst.of(DataTypes.DOM_NODE))
                .stage(TransformNthChild.of("td", 1))
                .stage(TransformNodeText.create())
                .stage(TransformRegexExtract.of("\\d+"))
                .stage(TransformParseInt.create())
            )
            .build();

        Map<String, Object> result = branch.execute(PipelineContext.empty(), rows);

        assertThat(result, hasEntry(equalTo("dmg"), equalTo(500)));
        assertThat(result, hasEntry(equalTo("strength"), equalTo(220)));
        assertThat(result, hasEntry(equalTo("crit_damage"), equalTo(175)));
    }

    @Test
    @DisplayName("Branch outputType is BRANCH_OUTPUT and kind is BRANCH")
    void branchAdvertisesContract() {
        Branch<String> branch = Branch.over(DataTypes.STRING).build();
        assertThat(branch.outputType(), is(DataTypes.BRANCH_OUTPUT));
        assertThat(branch.kind().name(), is(equalTo("BRANCH")));
    }

}
