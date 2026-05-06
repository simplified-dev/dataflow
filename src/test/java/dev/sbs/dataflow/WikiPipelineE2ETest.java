package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.collect.FirstCollect;
import dev.sbs.dataflow.stage.filter.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.NthChildTransform;
import dev.sbs.dataflow.stage.transform.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.RegexExtractTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class WikiPipelineE2ETest {

    @Test
    @DisplayName("Full chain - Paste(HTML) -> ParseHtml -> CssSelect -> Filter Dmg -> First -> Nth -> Text -> Regex -> ParseInt - yields 500")
    void darkClaymoreDmgIsFiveHundred() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.html(Fixtures.load("dark_claymore.html")))
            .stage(ParseHtmlTransform.create())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(DomTextContainsFilter.of("Dmg"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(NthChildTransform.of("td", 1))
            .stage(NodeTextTransform.create())
            .stage(RegexExtractTransform.of("\\d+"))
            .stage(ParseIntTransform.create())
            .build();

        assertThat(pipeline.validate().isValid(), is(true));
        Object result = pipeline.execute(PipelineContext.empty());
        assertThat(result, is(equalTo(500)));
    }

}
