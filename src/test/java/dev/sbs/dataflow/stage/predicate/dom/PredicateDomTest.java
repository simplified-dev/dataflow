package dev.sbs.dataflow.stage.predicate.dom;

import dev.sbs.dataflow.PipelineContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PredicateDomTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Text contains")
    void textContains() {
        Element el = Jsoup.parse("<p>hello world</p>").selectFirst("p");
        assertThat(DomTextContainsPredicate.of("world").execute(this.ctx, el), is(true));
        assertThat(DomTextContainsPredicate.of("xyz").execute(this.ctx, el), is(false));
        assertThat(DomTextContainsPredicate.of("world").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Text matches regex")
    void textMatches() {
        Element el = Jsoup.parse("<p>price 42</p>").selectFirst("p");
        assertThat(DomTextMatchesPredicate.of("\\d+").execute(this.ctx, el), is(true));
        assertThat(DomTextMatchesPredicate.of("zzz").execute(this.ctx, el), is(false));
    }

    @Test
    @DisplayName("Has attr (with and without expected value)")
    void hasAttr() {
        Element el = Jsoup.parse("<a href='https://example.com' class='primary'>x</a>").selectFirst("a");
        assertThat(DomHasAttrPredicate.of("href").execute(this.ctx, el), is(true));
        assertThat(DomHasAttrPredicate.of("missing").execute(this.ctx, el), is(false));
        assertThat(DomHasAttrPredicate.of("class", "primary").execute(this.ctx, el), is(true));
        assertThat(DomHasAttrPredicate.of("class", "secondary").execute(this.ctx, el), is(false));
    }

    @Test
    @DisplayName("Tag equals (case-insensitive)")
    void tagEquals() {
        Element el = Jsoup.parse("<DIV>x</DIV>").selectFirst("div");
        assertThat(DomTagEqualsPredicate.of("div").execute(this.ctx, el), is(true));
        assertThat(DomTagEqualsPredicate.of("DIV").execute(this.ctx, el), is(true));
        assertThat(DomTagEqualsPredicate.of("p").execute(this.ctx, el), is(false));
    }

}
