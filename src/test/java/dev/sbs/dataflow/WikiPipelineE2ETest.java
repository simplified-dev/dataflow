package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
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

        // T is inferred from the assignment - no explicit cast needed.
        Integer dmg = pipeline.execute(PipelineContext.empty());
        assertThat(dmg, is(equalTo(500)));
    }

}
