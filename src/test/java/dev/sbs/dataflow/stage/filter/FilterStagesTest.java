package dev.sbs.dataflow.stage.filter;

import dev.sbs.dataflow.stage.filter.dom.*;
import dev.sbs.dataflow.stage.filter.json.*;
import dev.sbs.dataflow.stage.filter.list.*;
import dev.sbs.dataflow.stage.filter.numeric.*;
import dev.sbs.dataflow.stage.filter.string.*;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class FilterStagesTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("DomTextContainsFilter keeps only nodes whose text contains the needle")
    void filterDomTextContains() {
        Element doc = Jsoup.parse("<div><p>has foo</p><p>nope</p><p>foo and bar</p></div>");
        List<Element> all = doc.select("p").stream().toList();
        assertThat(all.size(), is(equalTo(3)));

        List<Element> matches = DomTextContainsFilter.of("foo").execute(this.ctx, all);
        assertThat(matches, is(notNullValue()));
        assertThat(matches.size(), is(equalTo(2)));
        assertThat(matches.get(0).text(), is(equalTo("has foo")));
    }

    @Test
    @DisplayName("DomTextContainsFilter over an empty input is empty")
    void filterEmptyInput() {
        List<Element> result = DomTextContainsFilter.of("anything").execute(this.ctx, List.of());
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
