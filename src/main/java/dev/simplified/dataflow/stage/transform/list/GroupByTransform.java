package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
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
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link TransformStage} that groups JSON rows by the value of a key field and folds each group
 * into one new row.
 * <p>
 * Groups appear in the order their key first occurs. A row with no key field, or whose key is
 * JSON {@code null}, is dropped. Keys compare as JSON values: a number by its exact value, so
 * {@code 1}, {@code 1.0} and {@code 1e0} are one key while {@code "1"} is another and two integers
 * past double precision stay two, and an object by its members whatever their order. The folds
 * that look for equal values, {@link Aggregate#UNION} and {@link Aggregate#MODE}, compare values
 * the same way.
 * <p>
 * A folded row carries every field its group's rows carry, in the order the fields first appear,
 * then each field the aggregates table names that no row carries, in the table's order. A field
 * the table does not name keeps its first non-null value; a named field is folded by its
 * {@link Aggregate}. A fold with nothing to produce omits its field, as an
 * {@link ObjectBuildTransform} output omits a {@code null}. A JSON {@code null} is never a value:
 * every fold skips it.
 * <p>
 * The input rows are never changed. Every folded row, and every value in it, is a new tree.
 */
@StageSpec(
    id = "TRANSFORM_GROUP_BY",
    displayName = "Group by key",
    description = "List<JSON_OBJECT> -> List<JSON_OBJECT>",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GroupByTransform implements TransformStage<List<JsonObject>, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);

    /**
     * Field whose value groups the rows.
     */
    private final @NotNull String keyField;

    /**
     * Configured aggregates table exactly as given, carried on the wire under
     * {@code aggregates}, or {@code null} when the slot is absent.
     */
    private final @Nullable Map<String, String> rawAggregates;

    /**
     * Fold applied to each field the aggregates table names, in the table's order.
     */
    private final @NotNull Map<String, Aggregate> aggregates;

    /**
     * Fold that turns the values one field takes across a group's rows into the folded row's
     * value.
     * <p>
     * Every fold reads the field's non-null values in row order and skips a row that lacks the
     * field or carries JSON {@code null} there.
     */
    public enum Aggregate {

        /**
         * The first value, the fold a field the aggregates table does not name receives.
         */
        FIRST,

        /**
         * The last value.
         */
        LAST,

        /**
         * Every value in row order, as an array; an array value is one element of it.
         */
        LIST,

        /**
         * The distinct values in first-occurrence order, as an array; an array value contributes
         * its non-null elements.
         */
        UNION,

        /**
         * Every value in row order, as an array; an array value contributes its non-null
         * elements.
         */
        CONCAT,

        /**
         * The greatest JSON number, the earliest on a tie; a value that is not a JSON number is
         * skipped.
         */
        MAX,

        /**
         * The least JSON number, the earliest on a tie; a value that is not a JSON number is
         * skipped.
         */
        MIN,

        /**
         * The number of rows in the group, whether or not they carry the field.
         */
        COUNT,

        /**
         * The most frequent value, the earliest to occur on a tie.
         */
        MODE;

        /**
         * Folds the values {@code field} takes across one group's rows.
         *
         * @param rows the group's rows, in input order
         * @param field the field to fold
         * @return the folded value as a new tree, or {@code null} when there is nothing to produce
         */
        @Nullable JsonElement fold(@NotNull List<JsonObject> rows, @NotNull String field) {
            List<JsonElement> values = new ArrayList<>(rows.size());

            for (JsonObject row : rows) {
                JsonElement value = row.get(field);
                if (value != null && !value.isJsonNull()) values.add(value);
            }

            return switch (this) {
                case FIRST -> values.isEmpty() ? null : values.getFirst().deepCopy();
                case LAST -> values.isEmpty() ? null : values.getLast().deepCopy();
                case LIST -> array(values);
                case UNION -> array(distinct(flatten(values)));
                case CONCAT -> array(flatten(values));
                case MAX -> extreme(values, 1);
                case MIN -> extreme(values, -1);
                case COUNT -> new JsonPrimitive(rows.size());
                case MODE -> mode(values);
            };
        }

        private static @NotNull Aggregate parse(@NotNull String field, @NotNull String name) {
            for (Aggregate aggregate : values())
                if (aggregate.name().equals(name)) return aggregate;

            throw new IllegalArgumentException(String.format(
                "Invalid GroupByTransform aggregate '%s' for field '%s', expected one of %s",
                name, field, Arrays.toString(values())
            ));
        }

        private static @NotNull List<JsonElement> flatten(@NotNull List<JsonElement> values) {
            List<JsonElement> flat = new ArrayList<>(values.size());

            for (JsonElement value : values) {
                if (!value.isJsonArray()) {
                    flat.add(value);
                    continue;
                }

                for (JsonElement element : value.getAsJsonArray())
                    if (!element.isJsonNull()) flat.add(element);
            }

            return flat;
        }

        private static @NotNull List<JsonElement> distinct(@NotNull List<JsonElement> values) {
            Map<Object, JsonElement> firsts = new LinkedHashMap<>();

            for (JsonElement value : values)
                firsts.putIfAbsent(identity(value), value);

            return new ArrayList<>(firsts.values());
        }

        private static @NotNull JsonArray array(@NotNull Collection<JsonElement> values) {
            JsonArray array = new JsonArray(values.size());

            for (JsonElement value : values)
                array.add(value.deepCopy());

            return array;
        }

        private static @Nullable JsonElement extreme(@NotNull List<JsonElement> values, int direction) {
            JsonPrimitive best = null;
            BigDecimal bestNumber = null;

            for (JsonElement value : values) {
                if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) continue;
                BigDecimal number;

                try {
                    number = value.getAsBigDecimal();
                } catch (NumberFormatException ex) {
                    continue;
                }

                if (best == null || number.compareTo(bestNumber) * direction > 0) {
                    best = value.getAsJsonPrimitive();
                    bestNumber = number;
                }
            }

            return best == null ? null : best.deepCopy();
        }

        private static @Nullable JsonElement mode(@NotNull List<JsonElement> values) {
            Map<Object, JsonElement> firsts = new LinkedHashMap<>();
            Map<Object, Integer> counts = new HashMap<>();

            for (JsonElement value : values) {
                Object identity = identity(value);
                firsts.putIfAbsent(identity, value);
                counts.merge(identity, 1, Integer::sum);
            }

            JsonElement best = null;
            int bestCount = 0;

            for (Map.Entry<Object, JsonElement> entry : firsts.entrySet()) {
                int count = counts.get(entry.getKey());

                if (count > bestCount) {
                    best = entry.getValue();
                    bestCount = count;
                }
            }

            return best == null ? null : best.deepCopy();
        }

    }

    /**
     * Reduces a JSON value to a hash key that two equal values share.
     * <p>
     * A number becomes its exact {@link BigDecimal} value without trailing zeros, an object the
     * map of its members' keys, and an array the list of its elements' keys; any other value is
     * its own key. Gson's own number equality goes through {@code double}, and its hash disagrees
     * with it across number representations, so neither keys a hash map soundly.
     *
     * @param value the value
     * @return the key
     */
    private static @NotNull Object identity(@NotNull JsonElement value) {
        return switch (value) {
            case JsonObject object -> {
                Map<String, Object> members = new HashMap<>();

                for (Map.Entry<String, JsonElement> member : object.entrySet())
                    members.put(member.getKey(), identity(member.getValue()));

                yield members;
            }
            case JsonArray array -> {
                List<Object> elements = new ArrayList<>(array.size());

                for (JsonElement element : array)
                    elements.add(identity(element));

                yield elements;
            }
            case JsonPrimitive primitive when primitive.isNumber() -> exact(primitive);
            default -> value;
        };
    }

    private static @NotNull Object exact(@NotNull JsonPrimitive number) {
        try {
            return number.getAsBigDecimal().stripTrailingZeros();
        } catch (NumberFormatException ex) {
            return number;
        }
    }

    /**
     * Constructs a group-by stage.
     *
     * @param keyField the field whose value groups the rows
     * @param rawAggregates field name to {@link Aggregate} name, or {@code null} to fold every
     *                      field to its first value; carried on the wire as {@code aggregates}
     * @return the stage
     * @throws IllegalArgumentException when an aggregate name is not an {@link Aggregate}, or the
     *         table names {@code keyField}
     */
    public static @NotNull GroupByTransform of(
        @Configurable(label = "Key field", placeholder = "id")
        @NotNull String keyField,
        @Configurable(name = "aggregates", label = "Aggregates (optional)", placeholder = "{\"mobs\":\"LIST\"}", optional = true)
        @Nullable Map<String, String> rawAggregates
    ) {
        Map<String, Aggregate> folds = new LinkedHashMap<>();

        if (rawAggregates != null) {
            for (Map.Entry<String, String> entry : rawAggregates.entrySet()) {
                if (entry.getKey().equals(keyField)) {
                    throw new IllegalArgumentException(String.format(
                        "Invalid GroupByTransform aggregates: the key field '%s' cannot be aggregated", keyField
                    ));
                }

                folds.put(entry.getKey(), Aggregate.parse(entry.getKey(), entry.getValue()));
            }
        }

        return new GroupByTransform(
            keyField,
            rawAggregates == null ? null : Concurrent.newUnmodifiableLinkedMap(rawAggregates),
            Concurrent.newUnmodifiableLinkedMap(folds)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        Map<Object, List<JsonObject>> groups = new LinkedHashMap<>();

        for (JsonObject row : input) {
            if (row == null) continue;
            JsonElement key = row.get(this.keyField);
            if (key == null || key.isJsonNull()) continue;
            groups.computeIfAbsent(identity(key), k -> new ArrayList<>()).add(row);
        }

        List<JsonObject> result = new ArrayList<>(groups.size());

        for (List<JsonObject> rows : groups.values())
            result.add(this.fold(rows));

        return Concurrent.newUnmodifiableList(result);
    }

    private @NotNull JsonObject fold(@NotNull List<JsonObject> rows) {
        Set<String> fields = new LinkedHashSet<>();

        for (JsonObject row : rows)
            fields.addAll(row.keySet());

        fields.addAll(this.aggregates.keySet());
        JsonObject folded = new JsonObject();

        for (String field : fields) {
            JsonElement value = this.aggregates.getOrDefault(field, Aggregate.FIRST).fold(rows, field);
            if (value != null) folded.add(field, value);
        }

        return folded;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> inputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "GroupBy '" + this.keyField + "' (" + this.aggregates.size()
            + " aggregate" + (this.aggregates.size() == 1 ? "" : "s") + ")";
    }

}
