package dev.simplified.dataflow.stage.transform.dom;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.terminal.collect.NthCollect;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;
import org.jsoup.select.Evaluator;
import org.jsoup.select.QueryParser;
import org.jsoup.select.Selector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link TransformStage} that lays a {@code table} element out as a grid, placing each cell at
 * every position its {@code rowspan} and {@code colspan} cover.
 * <p>
 * It returns one list per row, in document order, with one entry per grid column as HTML's table
 * model counts them: a cell takes the first column that no cell from an earlier row still covers,
 * and a cell spanning several rows or columns appears at every position it covers. Column
 * {@code k} of every row is then a {@link MapTransform} with {@link NthCollect} {@code k} in its
 * body. The entries are the table's own nodes, so the DOM stages read their text, links and
 * attributes.
 * <p>
 * The rows returned are the table's own - its {@code tr} children and those of its {@code thead},
 * {@code tbody} and {@code tfoot} children, which leaves out the rows of a nested table - or the
 * elements {@link #rowSelector} selects within it. The cells of a row are its {@code td} and
 * {@code th} children. A row is laid out with the rest of its row group - the {@code tr} children
 * of its parent element, and any other child of it the selector selects - so a span covers every
 * row HTML shows it covering, a row the selector leaves out among them.
 * <p>
 * Spans are read as HTML reads them. A missing, malformed or zero {@code colspan} counts as 1 and
 * one above {@value #MAX_COLSPAN} as {@value #MAX_COLSPAN}; a missing, malformed or negative
 * {@code rowspan} counts as 1 and one above {@value #MAX_ROWSPAN} as {@value #MAX_ROWSPAN}, and a
 * {@code rowspan} of zero, written {@code "0"} or {@code "-0"}, covers the rest of its row group.
 * A span covers rows of its own row group only, so rows of another group laid out between them,
 * such as a nested table's rows a selector reaches, neither take nor end it, and it is cut at the
 * last row of its group. Where two cells claim one position, the one placed first keeps it.
 * <p>
 * A row is not padded to the width of the grid: it ends at the last position a cell covers, so a
 * short row stays short. A position before that which no cell covers - a column between a row's
 * own cells and a cell a {@code rowspan} carries into a later column - holds an empty {@code td}
 * outside the document, one element shared by every such position in the grid, so no entry is
 * {@code null} and a carried cell keeps its column.
 * <p>
 * Returns {@code null} when the input is not a {@code table} element, or when its rows lay out to
 * more than {@value #MAX_ENTRIES} positions in all, counting every row laid out - a guard against
 * a very large grid.
 */
@StageSpec(
    id = "TRANSFORM_DOM_SPAN_EXPAND",
    displayName = "Table span expand",
    description = "DOM_NODE -> List<List<DOM_NODE>>",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SpanExpandTransform implements TransformStage<Element, List<List<Element>>> {

    /**
     * Widest {@code colspan} read, as HTML clamps it.
     */
    public static final int MAX_COLSPAN = 1000;

    /**
     * Tallest {@code rowspan} read, as HTML clamps it.
     */
    public static final int MAX_ROWSPAN = 65534;

    /**
     * Most positions a table's rows are laid out to, across every row laid out.
     */
    public static final int MAX_ENTRIES = 1_000_000;

    private static final @NotNull DataType<List<List<Element>>> OUTPUT = DataType.list(DataType.list(DataTypes.DOM_NODE));

    private static final @NotNull Set<String> ROW_GROUPS = Set.of("thead", "tbody", "tfoot");

    /**
     * Configured CSS selector choosing the rows within the table, or {@code null} for the table's
     * own rows.
     */
    private final @Nullable String rowSelector;

    /**
     * {@link #rowSelector} compiled once, or {@code null} when it is absent. Every select through
     * it is followed by one over a detached empty element, since jsoup clears a compiled
     * evaluator's per-thread record of a select only when the next select starts, and that record
     * holds the selected table's document.
     */
    private final @Nullable Evaluator rowEvaluator;

    /**
     * Constructs a table span-expand stage.
     *
     * @param rowSelector the CSS selector choosing the rows within the table, or {@code null} for
     *                    the table's own rows
     * @return the stage
     * @throws IllegalArgumentException when {@code rowSelector} is not a valid CSS selector
     */
    public static @NotNull SpanExpandTransform of(
        @Configurable(label = "Row selector (optional)", placeholder = "tr", optional = true)
        @Nullable String rowSelector
    ) {
        return new SpanExpandTransform(rowSelector, rowSelector == null ? null : compile(rowSelector));
    }

    private static @NotNull Evaluator compile(@NotNull String rowSelector) {
        try {
            return QueryParser.parse(rowSelector);
        } catch (Selector.SelectorParseException ex) {
            throw new IllegalArgumentException(
                String.format("SpanExpandTransform rowSelector '%s' is not a valid CSS selector", rowSelector), ex
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<List<Element>> execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null || !"table".equals(input.normalName())) return null;
        List<Element> rows = this.rowEvaluator == null ? ownRows(input) : select(input, this.rowEvaluator);
        Set<Element> selected = Collections.newSetFromMap(new IdentityHashMap<>());
        selected.addAll(rows);
        Map<Element, List<Element>> layouts = new IdentityHashMap<>();
        Element gap = new Element("td");
        int budget = MAX_ENTRIES;

        for (Element row : rows) {
            if (layouts.containsKey(row)) continue;
            List<Span> spans = new ArrayList<>();

            for (Element member : group(row, selected)) {
                List<Element> layout = layOut(member, spans, gap, budget);
                if (layout == null) return null;
                budget -= layout.size();
                if (selected.contains(member)) layouts.put(member, layout);
            }
        }

        List<List<Element>> grid = new ArrayList<>(rows.size());

        for (Element row : rows)
            grid.add(layouts.get(row));

        return Concurrent.newUnmodifiableList(grid);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<List<Element>>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return this.rowSelector == null
            ? "Span expand table rows"
            : "Span expand rows '" + this.rowSelector + "'";
    }

    /**
     * Selects the rows {@code evaluator} picks within the table, then selects over a detached
     * empty element, so the evaluator's per-thread record of the select holds nothing of the
     * table's document once it returns.
     *
     * @param table the table element
     * @param evaluator the compiled row selector
     * @return the selected rows, in document order
     */
    private static @NotNull List<Element> select(@NotNull Element table, @NotNull Evaluator evaluator) {
        try {
            return table.select(evaluator);
        } finally {
            new Element("table").select(evaluator);
        }
    }

    /**
     * Lists the rows laid out with {@code row}: the {@code tr} children of its parent element and
     * any other child of it among {@code selected}, in document order, or {@code row} alone when
     * it has no parent.
     *
     * @param row the row
     * @param selected the rows returned
     * @return the rows of its row group
     */
    private static @NotNull List<Element> group(@NotNull Element row, @NotNull Set<Element> selected) {
        Element parent = row.parent();
        if (parent == null) return List.of(row);

        return parent.children()
            .stream()
            .filter(member -> "tr".equals(member.normalName()) || selected.contains(member))
            .toList();
    }

    /**
     * Lays out one row: the cells earlier rows carry into it, then its own cells in the columns
     * left free, then {@code gap} at every position before its last that no cell covers. Records
     * the rows each of its own spanning cells still covers in {@code spans}.
     *
     * @param row the row
     * @param spans the cell each column carries into the rows below, indexed by column
     * @param gap the cell standing at a position no cell covers
     * @param budget the most positions the row may take
     * @return the row's cells, one per grid column up to the last position a cell covers, or
     *         {@code null} when the row takes more than {@code budget} positions
     */
    private static @Nullable List<Element> layOut(@NotNull Element row, @NotNull List<Span> spans, @NotNull Element gap, int budget) {
        if (spans.size() > budget) return null;
        List<Element> slots = new ArrayList<>();

        for (int column = 0; column < spans.size(); column++) {
            Span span = spans.get(column);
            if (span == null) continue;
            place(slots, column, span.cell());
            spans.set(column, span.rowsLeft() > 1 ? new Span(span.cell(), span.rowsLeft() - 1) : null);
        }

        while (!spans.isEmpty() && spans.getLast() == null)
            spans.removeLast();

        int column = 0;

        for (Element cell : row.children()) {
            if (!isCell(cell)) continue;

            while (column < slots.size() && slots.get(column) != null)
                column++;

            int colspan = colspan(cell);
            int rowspan = rowspan(cell);
            if (column + colspan > budget) return null;

            for (int covered = column; covered < column + colspan; covered++) {
                if (covered < slots.size() && slots.get(covered) != null) continue;
                place(slots, covered, cell);
                if (rowspan != 1) place(spans, covered, new Span(cell, rowspan == 0 ? Integer.MAX_VALUE : rowspan - 1));
            }

            column += colspan;
        }

        slots.replaceAll(slot -> slot == null ? gap : slot);
        return Concurrent.newUnmodifiableList(slots);
    }

    /**
     * Lists the table's own rows: its {@code tr} children and the {@code tr} children of its
     * {@code thead}, {@code tbody} and {@code tfoot} children, in document order.
     *
     * @param table the table element
     * @return the rows
     */
    private static @NotNull List<Element> ownRows(@NotNull Element table) {
        List<Element> rows = new ArrayList<>();

        for (Element child : table.children()) {
            if ("tr".equals(child.normalName()))
                rows.add(child);
            else if (ROW_GROUPS.contains(child.normalName()))
                child.children().stream().filter(row -> "tr".equals(row.normalName())).forEach(rows::add);
        }

        return rows;
    }

    private static boolean isCell(@NotNull Element element) {
        return "td".equals(element.normalName()) || "th".equals(element.normalName());
    }

    private static int colspan(@NotNull Element cell) {
        int value = span(cell.attr("colspan"));
        return value <= 0 ? 1 : Math.min(value, MAX_COLSPAN);
    }

    private static int rowspan(@NotNull Element cell) {
        int value = span(cell.attr("rowspan"));
        return value < 0 ? 1 : Math.min(value, MAX_ROWSPAN);
    }

    /**
     * Reads a span attribute as HTML parses a non-negative integer: leading whitespace is skipped,
     * then a {@code +} or {@code -} sign, and the digits that follow are read, whatever comes
     * after them. A value below zero is refused, so {@code "-0"} reads as 0 and {@code "-2"} as
     * no value.
     *
     * @param value the attribute value, empty when the attribute is absent
     * @return the value, capped above either span limit, or {@code -1} when no digit follows the
     *         sign or the value is below zero
     */
    private static int span(@NotNull String value) {
        int index = 0;

        while (index < value.length() && " \t\n\f\r".indexOf(value.charAt(index)) >= 0)
            index++;

        boolean negative = index < value.length() && value.charAt(index) == '-';

        if (negative || index < value.length() && value.charAt(index) == '+')
            index++;

        int result = -1;

        for (; index < value.length() && value.charAt(index) >= '0' && value.charAt(index) <= '9'; index++)
            result = Math.min(Math.max(result, 0) * 10 + (value.charAt(index) - '0'), MAX_ROWSPAN + 1);

        return negative && result != 0 ? -1 : result;
    }

    private static <T> void place(@NotNull List<T> slots, int index, @NotNull T value) {
        while (slots.size() <= index)
            slots.add(null);

        slots.set(index, value);
    }

    /**
     * A cell carried into the rows below the one it starts in.
     *
     * @param cell the spanning cell
     * @param rowsLeft the rows below still to be covered
     */
    private record Span(@NotNull Element cell, int rowsLeft) {}

}
