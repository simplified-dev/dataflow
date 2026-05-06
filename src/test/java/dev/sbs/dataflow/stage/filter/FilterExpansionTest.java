package dev.sbs.dataflow.stage.filter;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class FilterExpansionTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("String filters cover contains/matches/startsWith/endsWith/equals/nonEmpty")
    void stringFilters() {
        List<String> all = List.of("apple", "banana", "apricot", "", "applepie");
        assertThat(StringContainsFilter.of("app").execute(this.ctx, all), contains("apple", "applepie"));
        assertThat(StringMatchesFilter.of("^ap").execute(this.ctx, all), contains("apple", "apricot", "applepie"));
        assertThat(StringStartsWithFilter.of("ap").execute(this.ctx, all), contains("apple", "apricot", "applepie"));
        assertThat(StringEndsWithFilter.of("ie").execute(this.ctx, all), contains("applepie"));
        assertThat(StringEqualsFilter.of("apple").execute(this.ctx, all), contains("apple"));
        assertThat(StringNonEmptyFilter.create().execute(this.ctx, all), contains("apple", "banana", "apricot", "applepie"));
    }

    @Test
    @DisplayName("DOM filters cover textMatches/hasAttr/tagEquals")
    void domFilters() {
        Element doc = Jsoup.parse(
            "<div>" +
                "<a href='/x' class='primary'>1</a>" +
                "<a class='primary'>2</a>" +
                "<span href='/y'>3</span>" +
                "<a href='/z'>4</a>" +
            "</div>");
        List<Element> nodes = doc.select("a, span").stream().toList();

        assertThat(DomTextMatchesFilter.of("^[1-3]$").execute(this.ctx, nodes), hasSize(3));
        assertThat(DomHasAttrFilter.of("href").execute(this.ctx, nodes), hasSize(3));
        assertThat(DomHasAttrFilter.of("class", "primary").execute(this.ctx, nodes), hasSize(2));
        assertThat(DomTagEqualsFilter.of("span").execute(this.ctx, nodes), hasSize(1));
    }

    @Test
    @DisplayName("JSON filters cover hasField/fieldEquals")
    void jsonFilters() {
        JsonObject a = JsonParser.parseString("{\"name\":\"x\",\"rare\":true}").getAsJsonObject();
        JsonObject b = JsonParser.parseString("{\"name\":\"y\"}").getAsJsonObject();
        JsonObject c = JsonParser.parseString("{\"name\":\"x\"}").getAsJsonObject();
        List<JsonObject> all = List.of(a, b, c);

        assertThat(JsonHasFieldFilter.of("rare").execute(this.ctx, all), hasSize(1));
        assertThat(JsonFieldEqualsFilter.of("name", "x").execute(this.ctx, all), hasSize(2));
    }

    @Test
    @DisplayName("Numeric filters cover Int / Long / Double greaterThan / lessThan / inRange")
    void numericFilters() {
        List<Integer> ints = Arrays.asList(1, 5, 10, 15, 20);
        assertThat(IntGreaterThanFilter.of(10).execute(this.ctx, ints), contains(15, 20));
        assertThat(IntLessThanFilter.of(10).execute(this.ctx, ints), contains(1, 5));
        assertThat(IntInRangeFilter.of(5, 15).execute(this.ctx, ints), contains(5, 10, 15));

        List<Long> longs = Arrays.asList(1L, 5_000_000_000L, 10_000_000_000L, 20_000_000_000L);
        assertThat(LongGreaterThanFilter.of(7_000_000_000L).execute(this.ctx, longs), contains(10_000_000_000L, 20_000_000_000L));
        assertThat(LongLessThanFilter.of(7_000_000_000L).execute(this.ctx, longs), contains(1L, 5_000_000_000L));
        assertThat(LongInRangeFilter.of(5_000_000_000L, 10_000_000_000L).execute(this.ctx, longs), contains(5_000_000_000L, 10_000_000_000L));

        List<Double> doubles = Arrays.asList(1.0, 2.5, 5.0, 7.5);
        assertThat(DoubleGreaterThanFilter.of(2.5).execute(this.ctx, doubles), contains(5.0, 7.5));
        assertThat(DoubleLessThanFilter.of(2.5).execute(this.ctx, doubles), contains(1.0));
        assertThat(DoubleInRangeFilter.of(1.0, 5.0).execute(this.ctx, doubles), contains(1.0, 2.5, 5.0));
    }

    @Test
    @DisplayName("Generic list filters cover NotNull / Take / Skip / IndexInRange")
    void genericListFilters() {
        List<String> data = Arrays.asList("a", null, "b", null, "c");
        assertThat(NotNullFilter.of(DataTypes.STRING).execute(this.ctx, data), contains("a", "b", "c"));

        List<String> abcde = List.of("a", "b", "c", "d", "e");
        assertThat(TakeFilter.of(DataTypes.STRING, 3).execute(this.ctx, abcde), contains("a", "b", "c"));
        assertThat(SkipFilter.of(DataTypes.STRING, 3).execute(this.ctx, abcde), contains("d", "e"));
        assertThat(IndexInRangeFilter.of(DataTypes.STRING, 1, 4).execute(this.ctx, abcde), contains("b", "c", "d"));

        // Out-of-range clamps gracefully
        assertThat(TakeFilter.of(DataTypes.STRING, 100).execute(this.ctx, abcde), hasSize(5));
        assertThat(SkipFilter.of(DataTypes.STRING, 100).execute(this.ctx, abcde), is(empty()));
        assertThat(IndexInRangeFilter.of(DataTypes.STRING, 10, 20).execute(this.ctx, abcde), is(empty()));
    }

    @Test
    @DisplayName("Filter summary strings include configuration values")
    void filterSummaries() {
        assertThat(StringContainsFilter.of("foo").summary(), equalTo("Contains 'foo'"));
        assertThat(IntInRangeFilter.of(1, 9).summary(), equalTo("Int in [1, 9]"));
        assertThat(DomHasAttrFilter.of("class", "x").summary(), equalTo("Attr class='x'"));
        assertThat(JsonFieldEqualsFilter.of("k", "v").summary(), equalTo("Field k='v'"));
    }

}
