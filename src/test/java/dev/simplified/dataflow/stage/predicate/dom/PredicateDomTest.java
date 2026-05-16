package dev.simplified.dataflow.stage.predicate.dom;

import dev.simplified.dataflow.PipelineContext;
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
        assertThat(TextContainsPredicate.of("world").execute(this.ctx, el), is(true));
        assertThat(TextContainsPredicate.of("xyz").execute(this.ctx, el), is(false));
        assertThat(TextContainsPredicate.of("world").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Text matches regex")
    void textMatches() {
        Element el = Jsoup.parse("<p>price 42</p>").selectFirst("p");
        assertThat(TextMatchesPredicate.of("\\d+").execute(this.ctx, el), is(true));
        assertThat(TextMatchesPredicate.of("zzz").execute(this.ctx, el), is(false));
    }

    @Test
    @DisplayName("Has attr (with and without expected value)")
    void hasAttr() {
        Element el = Jsoup.parse("<a href='https://example.com' class='primary'>x</a>").selectFirst("a");
        assertThat(HasAttrPredicate.of("href").execute(this.ctx, el), is(true));
        assertThat(HasAttrPredicate.of("missing").execute(this.ctx, el), is(false));
        assertThat(HasAttrPredicate.of("class", "primary").execute(this.ctx, el), is(true));
        assertThat(HasAttrPredicate.of("class", "secondary").execute(this.ctx, el), is(false));
    }

    @Test
    @DisplayName("Tag equals (case-insensitive)")
    void tagEquals() {
        Element el = Jsoup.parse("<DIV>x</DIV>").selectFirst("div");
        assertThat(TagEqualsPredicate.of("div").execute(this.ctx, el), is(true));
        assertThat(TagEqualsPredicate.of("DIV").execute(this.ctx, el), is(true));
        assertThat(TagEqualsPredicate.of("p").execute(this.ctx, el), is(false));
    }

}
