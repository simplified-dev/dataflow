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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * copied into {@code outputField}, and is left off when the root has no such value. The number
 * of parent steps to the root is written into {@code depthField} when one is set. A walk that
 * reaches a key it has already seen is a cycle: the row keeps neither field rather than taking a
 * guessed root.
 * <p>
 * Rows keep their order and are deep copies; the input rows are never mutated. A {@code null}
 * input rejects with {@code null}.
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
        Map<String, JsonObject> index = new HashMap<>();

        for (JsonObject row : input) {
            if (row == null) continue;
            String key = RowKeys.keyOf(row.get(this.keyField));
            if (key != null) index.putIfAbsent(key, row);
        }

        List<JsonObject> resolved = new ArrayList<>(input.size());

        for (JsonObject row : input) {
            if (row != null) resolved.add(this.resolve(row, index));
        }

        return Concurrent.newUnmodifiableList(resolved);
    }

    /**
     * Walks one row's parent chain and returns a copy of the row carrying its root.
     *
     * @param row the row to resolve
     * @param index the list's rows by key, first row per key
     * @return the resolved copy
     */
    private @NotNull JsonObject resolve(@NotNull JsonObject row, @NotNull Map<String, JsonObject> index) {
        JsonObject copy = row.deepCopy();
        Set<String> seen = new HashSet<>();
        String own = RowKeys.keyOf(row.get(this.keyField));
        if (own != null) seen.add(own);

        JsonObject current = row;
        int depth = 0;

        while (true) {
            JsonElement parentValue = current.get(this.parentField);
            if (!RowKeys.populated(parentValue)) break;
            String parentKey = RowKeys.keyOf(parentValue);
            JsonObject parent = index.get(parentKey);
            if (parent == null) break;
            if (!seen.add(parentKey)) return copy;
            current = parent;
            depth++;
        }

        JsonElement value = current.get(this.valueField == null ? this.keyField : this.valueField);
        if (value != null && !value.isJsonNull()) copy.add(this.outputField, value.deepCopy());
        if (this.depthField != null) copy.addProperty(this.depthField, depth);
        return copy;
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
