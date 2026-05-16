package dev.simplified.dataflow.stage.filter;

import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.filter.dom.TextContainsFilter;
import dev.simplified.dataflow.stage.filter.list.DistinctFilter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class FilterStagesTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("TextContainsFilter keeps only nodes whose text contains the needle")
    void filterDomTextContains() {
        Element doc = Jsoup.parse("<div><p>has foo</p><p>nope</p><p>foo and bar</p></div>");
        List<Element> all = doc.select("p").stream().toList();
        assertThat(all.size(), is(equalTo(3)));

        List<Element> matches = TextContainsFilter.of("foo").execute(this.ctx, all);
        assertThat(matches, is(notNullValue()));
        assertThat(matches.size(), is(equalTo(2)));
        assertThat(matches.getFirst().text(), is(equalTo("has foo")));
    }

    @Test
    @DisplayName("TextContainsFilter over an empty input is empty")
    void filterEmptyInput() {
        List<Element> result = TextContainsFilter.of("anything").execute(this.ctx, List.of());
        assertThat(result, is(empty()));
    }

    @Test
    @DisplayName("DistinctFilter preserves first-occurrence order")
    void filterDistinct() {
        List<String> result = DistinctFilter.of(DataTypes.STRING)
            .execute(this.ctx, List.of("a", "b", "a", "c", "b"));
        assertThat(result, contains("a", "b", "c"));
    }

}
