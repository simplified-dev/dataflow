package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.terminal.collect.FirstCollect;
import dev.sbs.dataflow.stage.source.OfSource;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPipelineBuilderTest {

    @Test
    @DisplayName("Builder.build() throws when stages have type-chain mismatches")
    void buildRejectsInvalidPipeline() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> DataPipeline.builder()
            .source(OfSource.html("<html></html>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("body"))
            .stage(ParseIntTransform.of())  // expects STRING; previous stage produces List<DOM_NODE>
            .build());
        assertThat(ex.getMessage(), containsString("invalid pipeline"));
        assertThat(ex.getMessage(), containsString("Stage #3"));
    }

    @Test
    @DisplayName("Builder.validate() returns the report without throwing")
    void builderValidateInspects() {
        ValidationReport report = DataPipeline.builder()
            .source(OfSource.html("<html></html>"))
            .stage(ParseHtmlTransform.of())
            .stage(ParseIntTransform.of())
            .validate();
        assertThat(report.isValid(), is(false));
    }

    @Test
    @DisplayName("Builder.build() succeeds for a well-typed chain")
    void buildAcceptsValidPipeline() {
        DataPipeline pipeline = DataPipeline.builder()
            .source(OfSource.html("<html><body><p>x</p></body></html>"))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("p"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .build();
        assertThat(pipeline.validate().isValid(), is(true));
    }

}
