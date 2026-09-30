package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TransformStage} that walks each row's parent chain through the list it is given and
 * writes the root of that chain onto the row.
 * <p>
 * A row follows {@code parentField} to the row whose {@code keyField} has the same string form -
 * a primitive's text, the compact JSON of an array or object - and on from there. When two rows
 * carry one key, the first is the one a parent reference reaches. The walk stops at a row whose
 * parent is absent, JSON null, {@code ""}, {@code []} or {@code {}}, or names a key the list
 * lacks; that row is the root, so a row with no parent is its own root at depth 0.
 * <p>
 * The root's {@code valueField}, or its {@code keyField} when no {@code valueField} is set, is
 * copied into {@code outputField}; when the root has no such value the row carries no
 * {@code outputField}, even one it held before. The number of parent steps to the root is
 * written into {@code depthField} when one is set. A walk that reaches a key it has already seen
 * is a cycle: the row carries neither field, even one it held before, rather than a guessed root.
 * <p>
 * The roots are found once for the whole list rather than by one walk per row, so a chain of any
 * depth costs one step per row. Rows keep their order and are deep copies; the input rows are
 * never mutated. A {@code null} input rejects with {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_RESOLVE_ANCESTOR",
    displayName = "Resolve ancestor",
    description = "List<JSON_OBJECT> -> List<JSON_OBJECT>",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ResolveAncestorTransform implements TransformStage<List<JsonObject>, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> ROWS = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull String keyField;

    private final @NotNull String parentField;

    private final @NotNull String outputField;

    private final @Nullable String valueField;

    private final @Nullable String depthField;

    /**
     * Constructs a stage resolving each row's root along {@code parentField}.
     *
     * @param keyField the field that keys a row
     * @param parentField the field naming a row's parent by its key
     * @param outputField the field the root's value is written into
     * @param valueField the root's field to copy, or {@code null} to copy its key
     * @param depthField the field the number of parent steps is written into, or {@code null} for none
     * @return the stage
     */
    public static @NotNull ResolveAncestorTransform of(
        @Configurable(label = "Key field", placeholder = "id")
        @NotNull String keyField,
        @Configurable(label = "Parent field", placeholder = "parent")
        @NotNull String parentField,
        @Configurable(label = "Output field", placeholder = "root")
        @NotNull String outputField,
        @Configurable(label = "Value field (optional)", placeholder = "name", optional = true)
        @Nullable String valueField,
        @Configurable(label = "Depth field (optional)", placeholder = "depth", optional = true)
        @Nullable String depthField
    ) {
        return new ResolveAncestorTransform(keyField, parentField, outputField, valueField, depthField);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        Forest forest = new Forest(input, this.keyField, this.parentField);
        List<JsonObject> resolved = new ArrayList<>(input.size());

        for (JsonObject row : input)
            if (row != null) resolved.add(this.resolve(row, forest));

        return Concurrent.newUnmodifiableList(resolved);
    }

    /**
     * Returns a copy of one row carrying its root.
     *
     * @param row the row to resolve
     * @param forest the list's keyed rows with their roots found
     * @return the resolved copy
     */
    private @NotNull JsonObject resolve(@NotNull JsonObject row, @NotNull Forest forest) {
        JsonObject copy = row.deepCopy();
        Root root = forest.rootOf(row);

        if (root == null) {
            copy.remove(this.outputField);
            if (this.depthField != null) copy.remove(this.depthField);
            return copy;
        }

        JsonElement value = root.row().get(this.valueField == null ? this.keyField : this.valueField);

        if (value == null || value.isJsonNull())
            copy.remove(this.outputField);
        else
            copy.add(this.outputField, value.deepCopy());

        if (this.depthField != null) copy.addProperty(this.depthField, root.depth());
        return copy;
    }

    /**
     * Row a parent chain ends at, and the number of parent steps taken to reach it.
     *
     * @param row the root row
     * @param depth the parent steps from the resolved row to the root
     */
    private record Root(@NotNull JsonObject row, int depth) {}

    /**
     * The list's keyed rows - the first row carrying each key - each linked to the keyed row its
     * parent names, with every root and depth found by one walk down from the roots.
     * <p>
     * A keyed row the walks never reach leads into a cycle. Each reached row also carries its
     * position in the walk and the size of the subtree below it, so whether one keyed row lies on
     * another's chain is answered without walking that chain.
     */
    private static final class Forest {

        /**
         * Field that keys a row.
         */
        private final @NotNull String keyField;

        /**
         * Field naming a row's parent by its key.
         */
        private final @NotNull String parentField;

        /**
         * Slot of each keyed row, by the string form of its key.
         */
        private final @NotNull Map<String, Integer> slots = new HashMap<>();

        /**
         * Keyed rows by slot, in list order.
         */
        private final @NotNull List<JsonObject> rows = new ArrayList<>();

        /**
         * Slot of each row's root, or {@code -1} for a row leading into a cycle.
         */
        private final int[] roots;

        /**
         * Parent steps from each reached row to its root.
         */
        private final int[] depths;

        /**
         * Position of each reached row in the walk from the roots, every subtree taking a
         * contiguous run of positions.
         */
        private final int[] positions;

        /**
         * Number of rows in each reached row's subtree, the row included.
         */
        private final int[] sizes;

        Forest(@NotNull List<JsonObject> input, @NotNull String keyField, @NotNull String parentField) {
            this.keyField = keyField;
            this.parentField = parentField;

            for (JsonObject row : input) {
                if (row == null) continue;
                String key = RowKeys.keyOf(row.get(keyField));
                if (key != null && this.slots.putIfAbsent(key, this.rows.size()) == null) this.rows.add(row);
            }

            int count = this.rows.size();
            this.roots = new int[count];
            this.depths = new int[count];
            this.positions = new int[count];
            this.sizes = new int[count];
            int[] parents = new int[count];
            int[] firstChild = new int[count];
            int[] nextSibling = new int[count];
            Arrays.fill(this.roots, -1);
            Arrays.fill(firstChild, -1);

            for (int slot = 0; slot < count; slot++) {
                int parent = this.parentOf(this.rows.get(slot));
                parents[slot] = parent;

                if (parent >= 0) {
                    nextSibling[slot] = firstChild[parent];
                    firstChild[parent] = slot;
                }
            }

            int[] pending = new int[count];
            int[] walk = new int[count];
            int reached = 0;

            for (int start = 0; start < count; start++) {
                if (parents[start] >= 0) continue;
                this.roots[start] = start;
                pending[0] = start;
                int top = 1;

                while (top > 0) {
                    int slot = pending[--top];
                    this.positions[slot] = reached;
                    walk[reached++] = slot;

                    for (int child = firstChild[slot]; child >= 0; child = nextSibling[child]) {
                        this.roots[child] = start;
                        this.depths[child] = this.depths[slot] + 1;
                        pending[top++] = child;
                    }
                }
            }

            for (int i = reached - 1; i >= 0; i--) {
                int slot = walk[i];
                this.sizes[slot]++;
                if (parents[slot] >= 0) this.sizes[parents[slot]] += this.sizes[slot];
            }
        }

        /**
         * Returns the slot of the keyed row a row's parent names.
         *
         * @param row the row
         * @return the slot, or {@code -1} when the parent is empty or names a key the list lacks
         */
        int parentOf(@NotNull JsonObject row) {
            JsonElement parent = row.get(this.parentField);
            if (!RowKeys.populated(parent)) return -1;
            Integer slot = this.slots.get(RowKeys.keyOf(parent));
            return slot == null ? -1 : slot;
        }

        /**
         * Returns the root a row's parent chain ends at.
         * <p>
         * A keyed row takes the root the walk found for it. Any other row - a later row repeating
         * a key, or a row with no key - steps to the keyed row its parent names and takes that
         * row's root one step further on, unless that chain leads into a cycle or passes through
         * the keyed row carrying the row's own key, which would reach that key a second time.
         *
         * @param row the row
         * @return the root, or {@code null} when the chain is a cycle
         */
        @Nullable Root rootOf(@NotNull JsonObject row) {
            String key = RowKeys.keyOf(row.get(this.keyField));
            Integer self = key == null ? null : this.slots.get(key);

            if (self != null && this.rows.get(self) == row)
                return this.roots[self] < 0 ? null : new Root(this.rows.get(this.roots[self]), this.depths[self]);

            int parent = this.parentOf(row);
            if (parent < 0) return new Root(row, 0);
            if (this.roots[parent] < 0 || self != null && this.onChain(self, parent)) return null;
            return new Root(this.rows.get(this.roots[parent]), this.depths[parent] + 1);
        }

        /**
         * Tests whether one keyed row lies on another's chain, the other itself included.
         *
         * @param ancestor the slot of the row looked for
         * @param slot the slot of a reached row whose chain is searched
         * @return {@code true} when {@code ancestor} is {@code slot} or one of its ancestors
         */
        private boolean onChain(int ancestor, int slot) {
            return this.roots[ancestor] >= 0
                && this.positions[ancestor] <= this.positions[slot]
                && this.positions[slot] < this.positions[ancestor] + this.sizes[ancestor];
        }

    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> inputType() {
        return ROWS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return ROWS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Resolve ancestor of '" + this.keyField + "' along '" + this.parentField + "' into '" + this.outputField + "'";
    }

}
