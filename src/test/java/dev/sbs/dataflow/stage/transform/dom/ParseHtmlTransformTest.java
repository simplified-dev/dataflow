package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.Fixtures;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class ParseHtmlTransformTest {

    private final ParseHtmlTransform stage = ParseHtmlTransform.of();

    @Test
    @DisplayName("Dark Claymore fixture parses into a queryable jsoup document")
    void parsesFixture() {
        Element root = stage.execute(PipelineContext.defaults(), Fixtures.load("dark_claymore.html"));
        assertThat(root, is(notNullValue()));
        assertThat(root.selectFirst("h1"), is(notNullValue()));
        assertThat(root.selectFirst("h1").text(), is(equalTo("Dark Claymore")));
        assertThat(root.select("table.infobox tr").size(), is(greaterThan(5)));
    }

    @Test
    @DisplayName("Null input passes through as null")
    void nullInputReturnsNull() {
        assertThat(stage.execute(PipelineContext.defaults(), null), is(nullValue()));
    }

}
