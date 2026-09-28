package dev.simplified.dataflow.stage.transform.dom;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.FirstCollect;
import dev.simplified.dataflow.stage.terminal.collect.NthCollect;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SpanExpandTransformTest {

    private static final @NotNull String STATS = """
        <table>
          <tr><th>Stat</th><th>Base</th><th>Cap</th></tr>
          <tr><td>Breaking Power</td><td rowspan="2">0</td><td>10</td></tr>
          <tr><td>Mining Spread</td><td>100</td></tr>
          <tr><td>Pristine</td><td>0</td><td>20</td></tr>
        </table>
        """;

    private final PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull Element table(@NotNull String html) {
        return Jsoup.parse(html).selectFirst("table");
    }

    private @Nullable List<List<Element>> expand(@Nullable String rowSelector, @NotNull Element table) {
        return SpanExpandTransform.of(rowSelector).execute(this.ctx, table);
    }

    private @NotNull List<List<String>> texts(@Nullable String rowSelector, @NotNull Element table) {
        return expand(rowSelector, table).stream()
            .map(row -> row.stream().map(Element::text).toList())
            .toList();
    }

    private @NotNull List<List<String>> texts(@NotNull String html) {
        return texts(null, table(html));
    }

    private static @NotNull DataPipeline<List<String>> column(@Nullable String rowSelector, int index) {
        return DataPipeline.builder()
            .source(LiteralSource.rawHtml(STATS))
            .stage(ParseHtmlTransform.of())
            .stage(CssSelectTransform.of("table"))
            .stage(FirstCollect.of(DataTypes.DOM_NODE))
            .stage(SpanExpandTransform.of(rowSelector))
            .stage(MapTransform.of(
                DataType.list(DataTypes.DOM_NODE),
                DataTypes.STRING,
                List.of(NthCollect.of(DataTypes.DOM_NODE, index), TextTransform.of())
            ))
            .build();
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(SpanExpandTransform.of(null).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("An element that is not a table rejects with null")
    void notTable() {
        assertThat(expand(null, Jsoup.parse("<div><p>x</p></div>").selectFirst("div")), is(nullValue()));
    }

    @Test
    @DisplayName("A table without spans lays out as written")
    void noSpans() {
        assertThat(texts("<table><tr><td>a</td><td>b</td></tr><tr><td>c</td><td>d</td></tr></table>"), is(equalTo(List.of(List.of("a", "b"), List.of("c", "d")))));
    }

    @Test
    @DisplayName("A rowspan cell appears in the rows it covers, in its own column")
    void rowspanCopiesDown() {
        assertThat(texts(STATS).get(2), is(equalTo(List.of("Mining Spread", "0", "100"))));
    }

    @Test
    @DisplayName("Rows below a rowspan's reach lay out as written")
    void rowspanEnds() {
        assertThat(texts(STATS).get(3), is(equalTo(List.of("Pristine", "0", "20"))));
    }

    @Test
    @DisplayName("A colspan cell appears at every column it covers")
    void colspanRepeats() {
        assertThat(texts("<table><tr><td colspan='3'>a</td><td>b</td></tr></table>"), is(equalTo(List.of(List.of("a", "a", "a", "b")))));
    }

    @Test
    @DisplayName("A cell spanning rows and columns fills its whole block")
    void blockSpan() {
        assertThat(
            texts("<table><tr><td rowspan='2' colspan='2'>a</td><td>b</td></tr><tr><td>c</td></tr></table>"),
            is(equalTo(List.of(List.of("a", "a", "b"), List.of("a", "a", "c"))))
        );
    }

    @Test
    @DisplayName("Every position a cell covers holds the same node")
    void positionsShareNode() {
        List<List<Element>> grid = expand(null, table("<table><tr><td rowspan='2'>a</td><td>b</td></tr><tr><td>c</td></tr></table>"));
        assertThat(grid.get(1).getFirst(), is(sameInstance(grid.getFirst().getFirst())));
    }

    @Test
    @DisplayName("A rowspan running past the last row is cut there")
    void rowspanCutAtLastRow() {
        assertThat(texts("<table><tr><td rowspan='5'>a</td><td>b</td></tr><tr><td>c</td></tr></table>").size(), is(equalTo(2)));
    }

    @Test
    @DisplayName("A short row stays short")
    void shortRowStaysShort() {
        assertThat(texts("<table><tr><td>a</td><td>b</td><td>c</td></tr><tr><td>d</td></tr></table>").get(1), is(equalTo(List.of("d"))));
    }

    @Test
    @DisplayName("A row ends at the first position no cell covers")
    void rowEndsAtGap() {
        assertThat(
            texts("<table><tr><td>a</td><td>b</td><td rowspan='2'>c</td></tr><tr><td>d</td></tr></table>").get(1),
            is(equalTo(List.of("d")))
        );
    }

    @Test
    @DisplayName("A row whose cells all come from above holds only those")
    void rowOfCarriedCells() {
        assertThat(
            texts("<table><tr><td rowspan='2'>a</td><td rowspan='2'>b</td></tr><tr></tr></table>").get(1),
            is(equalTo(List.of("a", "b")))
        );
    }

    @Test
    @DisplayName("rowspan=0 covers the rest of its row group")
    void rowspanZeroToGroupEnd() {
        assertThat(
            texts("<table><tr><td rowspan='0'>a</td><td>b</td></tr><tr><td>c</td></tr><tr><td>d</td></tr></table>"),
            is(equalTo(List.of(List.of("a", "b"), List.of("a", "c"), List.of("a", "d"))))
        );
    }

    @Test
    @DisplayName("A span does not run from one row group into the next")
    void spanStopsAtRowGroup() {
        assertThat(
            texts("<table><thead><tr><th rowspan='3'>h</th><th>x</th></tr></thead><tbody><tr><td>a</td><td>b</td></tr></tbody></table>").get(1),
            is(equalTo(List.of("a", "b")))
        );
    }

    @Test
    @DisplayName("A zero colspan counts as 1")
    void zeroColspan() {
        assertThat(texts("<table><tr><td colspan='0'>a</td><td>b</td></tr></table>").getFirst(), is(equalTo(List.of("a", "b"))));
    }

    @Test
    @DisplayName("A malformed span counts as 1")
    void malformedSpan() {
        assertThat(texts("<table><tr><td colspan='wide' rowspan='-2'>a</td><td>b</td></tr><tr><td>c</td></tr></table>"), is(equalTo(List.of(List.of("a", "b"), List.of("c")))));
    }

    @Test
    @DisplayName("A span is read from its leading digits, as HTML reads it")
    void spanLeadingDigits() {
        assertThat(texts("<table><tr><td colspan=' 2px'>a</td><td>b</td></tr></table>").getFirst(), is(equalTo(List.of("a", "a", "b"))));
    }

    @Test
    @DisplayName("A colspan above the limit is clamped to it")
    void colspanClamped() {
        assertThat(texts("<table><tr><td colspan='5000'>a</td></tr></table>").getFirst().size(), is(equalTo(SpanExpandTransform.MAX_COLSPAN)));
    }

    @Test
    @DisplayName("Header and data cells both count")
    void headerCellsCount() {
        assertThat(texts(STATS).getFirst(), is(equalTo(List.of("Stat", "Base", "Cap"))));
    }

    @Test
    @DisplayName("Rows keep document order across thead, tbody and tfoot")
    void rowGroupsInDocumentOrder() {
        assertThat(
            texts("<table><thead><tr><th>h</th></tr></thead><tbody><tr><td>b</td></tr></tbody><tfoot><tr><td>f</td></tr></tfoot></table>"),
            is(equalTo(List.of(List.of("h"), List.of("b"), List.of("f"))))
        );
    }

    @Test
    @DisplayName("The table's own rows leave out a nested table's rows")
    void nestedTableRowsExcluded() {
        assertThat(
            texts("<table><tr><td><table><tr><td>inner</td></tr></table></td><td>b</td></tr></table>").size(),
            is(equalTo(1))
        );
    }

    @Test
    @DisplayName("A tr directly under the table is one of its rows")
    void directRows() {
        Element table = Jsoup.parse("<table><tr><td>a</td></tr><tr><td>b</td></tr></table>", "", Parser.xmlParser()).selectFirst("table");
        assertThat(texts(null, table), is(equalTo(List.of(List.of("a"), List.of("b")))));
    }

    @Test
    @DisplayName("A row child that is not td or th is not a cell")
    void nonCellChildIgnored() {
        Element table = Jsoup.parse("<table><tr><td>a</td><span>x</span><th>b</th></tr></table>", "", Parser.xmlParser()).selectFirst("table");
        assertThat(texts(null, table), is(equalTo(List.of(List.of("a", "b")))));
    }

    @Test
    @DisplayName("A row selector chooses the rows, and spans count the chosen rows")
    void rowSelector() {
        assertThat(texts("tr:has(td)", table(STATS)), is(equalTo(List.of(
            List.of("Breaking Power", "0", "10"),
            List.of("Mining Spread", "0", "100"),
            List.of("Pristine", "0", "20")
        ))));
    }

    @Test
    @DisplayName("A row selector reaching a nested table's rows keeps the outer table's spans across them")
    void selectorKeepsSpansAcrossNestedRows() {
        Element outer = table("<table><tr><td rowspan='2'>a</td><td><table><tr><td>inner</td></tr></table></td></tr><tr><td>b</td></tr></table>");
        assertThat(texts("tr", outer).getLast(), is(equalTo(List.of("a", "b"))));
    }

    @Test
    @DisplayName("A row selector reaching a nested table's rows lays them out apart from the outer table's spans")
    void selectorLaysNestedRowsApart() {
        Element outer = table("<table><tr><td rowspan='2'>a</td><td><table><tr><td>inner</td></tr></table></td></tr><tr><td>b</td></tr></table>");
        assertThat(texts("tr", outer).get(1), is(equalTo(List.of("inner"))));
    }

    @Test
    @DisplayName("An empty table yields an empty grid")
    void emptyTable() {
        assertThat(texts("<table></table>"), is(empty()));
    }

    @Test
    @DisplayName("of rejects a row selector that does not parse")
    void invalidSelectorRejected() {
        assertThrows(IllegalArgumentException.class, () -> SpanExpandTransform.of("tr[["));
    }

    @Test
    @DisplayName("A wire row selector that does not parse fails the load")
    void invalidWireSelectorFailsLoad() {
        String json = "[{\"kind\": \"SOURCE_LITERAL\", \"outputType\": \"RAW_HTML\", \"value\": \"<table></table>\"},"
            + " {\"kind\": \"PARSE_HTML\"},"
            + " {\"kind\": \"TRANSFORM_CSS_SELECT\", \"selector\": \"table\"},"
            + " {\"kind\": \"COLLECT_FIRST\", \"elementType\": \"DOM_NODE\"},"
            + " {\"kind\": \"TRANSFORM_DOM_SPAN_EXPAND\", \"rowSelector\": \"tr[[\"}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(causeMessages(thrown), hasItem(containsString("SpanExpandTransform rowSelector 'tr[[' is not a valid CSS selector")));
    }

    private static @NotNull List<String> causeMessages(@NotNull Throwable thrown) {
        List<String> messages = new ArrayList<>();

        for (Throwable cause = thrown; cause != null && messages.size() < 16; cause = cause.getCause())
            messages.add(String.valueOf(cause.getMessage()));

        return messages;
    }

    @Test
    @DisplayName("The grid is unmodifiable")
    void gridUnmodifiable() {
        List<List<Element>> grid = expand(null, table(STATS));
        assertThrows(UnsupportedOperationException.class, () -> grid.add(List.of()));
    }

    @Test
    @DisplayName("Each row is unmodifiable")
    void rowUnmodifiable() {
        List<Element> row = expand(null, table(STATS)).getFirst();
        assertThrows(UnsupportedOperationException.class, () -> row.add(new Element("td")));
    }

    @Test
    @DisplayName("An absent row selector is left out of the configuration")
    void absentSelectorOmitted() {
        assertThat(SpanExpandTransform.of(null).config().has("rowSelector"), is(false));
    }

    @Test
    @DisplayName("Mapping NthCollect over the grid reads one column of every row")
    void readsColumn() {
        assertThat(column(null, 1).execute(), is(equalTo(List.of("Base", "0", "0", "0"))));
    }

    @Test
    @DisplayName("A pipeline without a row selector round-trips to the same JSON")
    void wireRoundTripDefault() {
        String first = PipelineGson.toJson(column(null, 2));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with a row selector round-trips to the same JSON")
    void wireRoundTripSelector() {
        String first = PipelineGson.toJson(column("tr:has(td)", 2));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("The wire form carries the row selector")
    void wireFormCarriesSelector() {
        assertThat(PipelineGson.toJson(column("tr:has(td)", 2)), containsString("{\"kind\":\"TRANSFORM_DOM_SPAN_EXPAND\",\"rowSelector\":\"tr:has(td)\"}"));
    }

    @Test
    @DisplayName("A pipeline round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<List<String>> pipeline = column("tr:has(td)", 2);
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline)).execute(), is(equalTo(pipeline.execute())));
    }

}
