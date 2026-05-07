package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.collect.FirstCollect;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

class DataTypeChainTest {

    @Test
    @DisplayName("Type mismatch reports stage index, kind, expected and produced types")
    void mismatchEmitsKindAndTypes() {
        // PasteSource.text emits RAW_TEXT; ParseHtmlTransform expects RAW_HTML.
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.text("hi"))
            .stage(ParseHtmlTransform.of())
            .build();

        ValidationReport report = pipeline.validate();
        assertThat(report.isValid(), is(false));
        ValidationReport.Issue issue = report.issues().get(0);
        assertThat(issue.stageIndex(), is(1));
        assertThat(issue.message(), containsString("Stage #1"));
        assertThat(issue.message(), containsString("PARSE_HTML"));
        assertThat(issue.message(), containsString("RAW_HTML"));
        assertThat(issue.message(), containsString("RAW_TEXT"));
    }

    @Test
    @DisplayName("Mismatch deeper in the chain reports the right index")
    void deeperMismatchReportsRightIndex() {
        // Source -> ParseHtml -> CssSelect (DOM_NODE -> List<DOM_NODE>) -> ParseInt (expects STRING) - mismatch at index 3
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.html("<html><body>x</body></html>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("body"))
            .stage(ParseIntTransform.of())
            .build();

        ValidationReport report = pipeline.validate();
        assertThat(report.isValid(), is(false));
        ValidationReport.Issue issue = report.issues().get(0);
        assertThat(issue.stageIndex(), is(3));
        assertThat(issue.message(), containsString("STRING"));
        assertThat(issue.message(), containsString("List<DOM_NODE>"));
    }

    @Test
    @DisplayName("Valid wiki chain reports no issues")
    void validChainReportsNoIssues() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(PasteSource.html("<table class='infobox'><tr><td>Dmg</td><td>500</td></tr></table>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table.infobox tr"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .build();
        assertThat(pipeline.validate().isValid(), is(true));
    }

}
