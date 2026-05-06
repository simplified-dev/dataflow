package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.collect.CollectFirst;
import dev.sbs.dataflow.stage.filter.FilterDomTextContains;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.TransformCssSelect;
import dev.sbs.dataflow.stage.transform.TransformNodeText;
import dev.sbs.dataflow.stage.transform.TransformNthChild;
import dev.sbs.dataflow.stage.transform.TransformParseInt;
import dev.sbs.dataflow.stage.transform.TransformRegexExtract;
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
            .stage(TransformCssSelect.of("table.infobox tr"))
            .stage(FilterDomTextContains.of("Dmg"))
            .stage(CollectFirst.of(DataTypes.DOM_NODE))
            .stage(TransformNthChild.of("td", 1))
            .stage(TransformNodeText.create())
            .stage(TransformRegexExtract.of("\\d+"))
            .stage(TransformParseInt.create())
            .build();

        assertThat(pipeline.validate().isValid(), is(true));
        Object result = pipeline.execute(PipelineContext.empty());
        assertThat(result, is(equalTo(500)));
    }

}
