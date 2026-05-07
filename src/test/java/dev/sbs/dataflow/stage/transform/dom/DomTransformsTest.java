package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.PipelineContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class DomTransformsTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("DomChildren returns the direct children")
    void domChildren() {
        Element div = Jsoup.parse("<div><p>a</p><p>b</p><span>c</span></div>").selectFirst("div");
        List<Element> kids = DomChildrenTransform.of().execute(this.ctx, div);
        assertThat(kids.size(), is(equalTo(3)));
        assertThat(kids.get(0).tagName(), is(equalTo("p")));
        assertThat(kids.get(2).tagName(), is(equalTo("span")));
    }

    @Test
    @DisplayName("DomParent returns the parent element")
    void domParent() {
        Element a = Jsoup.parse("<div><p><a>x</a></p></div>").selectFirst("a");
        Element parent = DomParentTransform.of().execute(this.ctx, a);
        assertThat(parent, is(notNullParent()));
        assertThat(parent.tagName(), is(equalTo("p")));
    }

    @Test
    @DisplayName("DomOuterHtml renders the element's outer markup")
    void domOuterHtml() {
        Element span = Jsoup.parse("<div><span class='x'>hi</span></div>").selectFirst("span");
        String html = DomOuterHtmlTransform.of().execute(this.ctx, span);
        assertThat(html, containsString("<span class=\"x\">"));
        assertThat(html, containsString("hi"));
    }

    @Test
    @DisplayName("DomOwnText returns only the direct text, excluding child text")
    void domOwnText() {
        Element p = Jsoup.parse("<p>direct <b>nested</b> tail</p>").selectFirst("p");
        String text = DomOwnTextTransform.of().execute(this.ctx, p);
        assertThat(text, is(equalTo("direct tail")));
    }

    private static org.hamcrest.Matcher<Element> notNullParent() {
        return org.hamcrest.Matchers.notNullValue(Element.class);
    }

}
