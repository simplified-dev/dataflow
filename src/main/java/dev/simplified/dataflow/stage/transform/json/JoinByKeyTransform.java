package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link TransformStage} that merges the rows of a second document, read through a pipeline
 * operand, into the running rows by key.
 * <p>
 * A left row matches the right row whose {@code rightKey} has the same string form as its
 * {@code leftKey} - a primitive's text, the compact JSON of an array or object. When two right
 * rows carry one key, the first is the one matched, and a right row with no key is never
 * matched. A matched row keeps every left value and takes a right value only into a cell that
 * is absent, JSON null, {@code ""}, {@code []} or {@code {}}, and only when the right value is
 * none of those itself; the right row's {@code rightKey} is never copied, and {@code columns},
 * when set, limits the right keys that may fill a cell.
 * <p>
 * {@link Mode#INNER} keeps the matched left rows, {@link Mode#LEFT} keeps every left row, and
 * {@link Mode#FULL} keeps every left row and then appends, in right order, each right row whose
 * key no left row carries, as a row holding its key under {@code leftKey} plus the cells the
 * same fill rule takes from it. Left rows keep their order. {@code FULL} joins chained over
 * inputs whose keys are unique give the rows of a merge by key that fills each cell from the
 * first input populating it.
 * <p>
 * Output rows are deep copies; neither the input rows nor the operand's rows are mutated, since
 * the operand is evaluated at most once per context and shared by every stage that reads it.
 * A {@code null} input or a {@code null} operand output rejects with {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_JOIN_BY_KEY",
    displayName = "Join by key",
    description = "List<JSON_OBJECT> -> List<JSON_OBJECT>",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JoinByKeyTransform implements TransformStage<List<JsonObject>, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> ROWS = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull String leftKey;

    private final @NotNull String rightKey;

    /**
     * Configured join mode exactly as given, carried on the wire under {@code mode}.
     */
    private final @NotNull String mode;

    /**
     * Configured comma-separated column list exactly as given, or {@code null} when every right
     * key may fill a cell.
     */
    private final @Nullable String columns;

    private final @NotNull DataPipeline<List<JsonObject>> right;

    /**
     * Join mode parsed from {@link #mode}.
     */
    private final @NotNull Mode joinMode;

    /**
     * Right keys parsed from {@link #columns}, or {@code null} when every right key may fill a cell.
     */
    private final @Nullable Set<String> columnSet;

    /**
     * The right rows indexed by {@link #rightKey}, built at most once per context.
     */
    @Getter(AccessLevel.NONE)
    private final @NotNull DataPipeline<Map<String, JsonObject>> rightIndex;

    /**
     * Which rows a join keeps.
     */
    public enum Mode {

        /**
         * Keeps the left rows that match a right row.
         */
        INNER,

        /**
         * Keeps every left row, matched or not.
         */
        LEFT,

        /**
         * Keeps every left row, then appends the right rows no left row matches.
         */
        FULL

    }

    /**
     * Constructs a join of the running rows with the rows {@code right} produces.
     *
     * @param leftKey the field of a running row that keys it
     * @param rightKey the field of an operand row that keys it
     * @param mode the {@link Mode} name - {@code INNER}, {@code LEFT} or {@code FULL}
     * @param columns comma-separated right keys that may fill a cell, or {@code null} for every key
     * @param right the operand pipeline, whose output must be {@code List<JSON_OBJECT>}
     * @return the stage
     * @throws IllegalArgumentException when {@code mode} names no {@link Mode}, {@code columns}
     *         names no key, or {@code right} is invalid or does not produce {@code List<JSON_OBJECT>}
     */
    @SuppressWarnings("unchecked")
    public static @NotNull JoinByKeyTransform of(
        @Configurable(label = "Left key", placeholder = "id")
        @NotNull String leftKey,
        @Configurable(label = "Right key", placeholder = "id")
        @NotNull String rightKey,
        @Configurable(label = "Mode (INNER, LEFT or FULL)", placeholder = "LEFT")
        @NotNull String mode,
        @Configurable(label = "Columns (optional)", placeholder = "name,gameType", optional = true)
        @Nullable String columns,
        @Configurable(label = "Right pipeline")
        @NotNull DataPipeline<?> right
    ) {
        Mode joinMode = parseMode(mode);
        Set<String> columnSet = columns == null ? null : parseColumns(columns);
        ValidationReport report = right.validate(ROWS);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid JoinByKeyTransform operand: " + report.issues());

        DataPipeline<List<JsonObject>> rows = (DataPipeline<List<JsonObject>>) right;
        return new JoinByKeyTransform(
            leftKey,
            rightKey,
            mode,
            columns,
            rows,
            joinMode,
            columnSet,
            RowKeys.index(rows, rightKey, "TRANSFORM_JOIN_BY_KEY")
        );
    }

    private static @NotNull Mode parseMode(@NotNull String mode) {
        for (Mode candidate : Mode.values()) {
            if (candidate.name().equals(mode))
                return candidate;
        }

        throw new IllegalArgumentException(
            "Unknown JoinByKeyTransform mode '" + mode + "', expected one of " + Arrays.toString(Mode.values())
        );
    }

    private static @NotNull Set<String> parseColumns(@NotNull String columns) {
        Set<String> parsed = new LinkedHashSet<>();

        for (String column : columns.split(",")) {
            if (!column.isBlank())
                parsed.add(column.trim());
        }

        if (parsed.isEmpty())
            throw new IllegalArgumentException("JoinByKeyTransform columns '" + columns + "' names no key");

        return Set.copyOf(parsed);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        Map<String, JsonObject> index = ctx.evaluateOperand(this.rightIndex);
        if (index == null) return null;

        List<JsonObject> joined = new ArrayList<>(input.size());
        Set<String> matched = new HashSet<>();

        for (JsonObject left : input) {
            if (left == null) continue;
            String key = RowKeys.keyOf(left.get(this.leftKey));
            JsonObject match = key == null ? null : index.get(key);

            if (match == null) {
                if (this.joinMode != Mode.INNER) joined.add(left.deepCopy());
                continue;
            }

            matched.add(key);
            joined.add(this.fill(left.deepCopy(), match));
        }

        if (this.joinMode == Mode.FULL) {
            for (Map.Entry<String, JsonObject> entry : index.entrySet()) {
                if (matched.contains(entry.getKey())) continue;
                JsonObject row = new JsonObject();
                row.add(this.leftKey, entry.getValue().get(this.rightKey).deepCopy());
                joined.add(this.fill(row, entry.getValue()));
            }
        }

        return Concurrent.newUnmodifiableList(joined);
    }

    /**
     * Copies into {@code row} each cell of {@code match} that the fill rule and the column list
     * allow.
     *
     * @param row the output row, already a copy
     * @param match the right row it matched
     * @return {@code row}
     */
    private @NotNull JsonObject fill(@NotNull JsonObject row, @NotNull JsonObject match) {
        for (Map.Entry<String, JsonElement> cell : match.entrySet()) {
            String column = cell.getKey();
            if (column.equals(this.rightKey)) continue;
            if (this.columnSet != null && !this.columnSet.contains(column)) continue;
            if (RowKeys.populated(row.get(column)) || !RowKeys.populated(cell.getValue())) continue;
            row.add(column, cell.getValue().deepCopy());
        }

        return row;
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
        return "Join " + this.mode + " on '" + this.leftKey + "' = '" + this.rightKey + "'"
            + (this.columns == null ? "" : " (" + this.columns + ")");
    }

}
