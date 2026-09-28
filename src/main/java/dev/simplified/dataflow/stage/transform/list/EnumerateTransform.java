package dev.simplified.dataflow.stage.transform.list;

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
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link TransformStage} that pairs each element of a list with its position, turning
 * {@code List<T>} into {@code List<JSON_OBJECT>}.
 * <p>
 * Each element becomes {@code {indexKey: start + position, valueKey: element}}, in input order.
 * The position is the element's index in the input list, so a {@code null} element, which is
 * dropped, still uses up its number; a JSON {@code null} element is dropped the same way. An
 * element is written as an {@link ObjectBuildTransform} output is, and a {@link JsonElement}
 * element is copied. Numbering that restarts per group is a {@link FlatMapTransform} over the
 * groups with this stage in its body.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "TRANSFORM_ENUMERATE",
    displayName = "Enumerate",
    description = "List<T> -> List<JSON_OBJECT>",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class EnumerateTransform<T> implements TransformStage<List<T>, List<JsonObject>> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Configured first index exactly as given, carried on the wire under {@code start}, or
     * {@code null} when the slot is absent.
     */
    private final @Nullable Integer rawStart;

    /**
     * Configured index key exactly as given, carried on the wire under {@code indexKey}, or
     * {@code null} when the slot is absent.
     */
    private final @Nullable String rawIndexKey;

    /**
     * Configured value key exactly as given, carried on the wire under {@code valueKey}, or
     * {@code null} when the slot is absent.
     */
    private final @Nullable String rawValueKey;

    /**
     * Index of the first element, {@code 0} unless configured.
     */
    private final int start;

    /**
     * Key each output object holds the index under, {@code index} unless configured.
     */
    private final @NotNull String indexKey;

    /**
     * Key each output object holds the element under, {@code value} unless configured.
     */
    private final @NotNull String valueKey;

    /**
     * Constructs an enumerate stage.
     *
     * @param elementType element type of the input list; a type Gson can write
     * @param rawStart index of the first element, or {@code null} for {@code 0}; carried on the
     *                 wire as {@code start}
     * @param rawIndexKey key for the index, or {@code null} for {@code index}; carried on the wire
     *                    as {@code indexKey}
     * @param rawValueKey key for the element, or {@code null} for {@code value}; carried on the
     *                    wire as {@code valueKey}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code elementType} cannot be written as JSON, or the
     *         index key and the value key are the same
     */
    public static <T> @NotNull EnumerateTransform<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(name = "start", label = "Start (optional)", placeholder = "0", optional = true)
        @Nullable Integer rawStart,
        @Configurable(name = "indexKey", label = "Index key (optional)", placeholder = "index", optional = true)
        @Nullable String rawIndexKey,
        @Configurable(name = "valueKey", label = "Value key (optional)", placeholder = "value", optional = true)
        @Nullable String rawValueKey
    ) {
        JsonValues.requireWritable(elementType, "EnumerateTransform", "elementType");
        String indexKey = rawIndexKey == null ? "index" : rawIndexKey;
        String valueKey = rawValueKey == null ? "value" : rawValueKey;

        if (indexKey.equals(valueKey))
            throw new IllegalArgumentException(String.format(
                "Invalid EnumerateTransform keys: the index and the value are both keyed '%s'", indexKey
            ));

        return new EnumerateTransform<>(
            elementType,
            DataType.list(elementType),
            rawStart,
            rawIndexKey,
            rawValueKey,
            rawStart == null ? 0 : rawStart,
            indexKey,
            valueKey
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<JsonObject> result = new ArrayList<>(input.size());
        long index = this.start;

        for (T element : input) {
            if (!JsonValues.isNull(element)) {
                JsonObject entry = new JsonObject();
                entry.addProperty(this.indexKey, index);
                entry.add(this.valueKey, JsonValues.toJson(element));
                result.add(entry);
            }

            index++;
        }

        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Enumerate " + this.elementType.label() + " from " + this.start
            + " as {" + this.indexKey + ", " + this.valueKey + "}";
    }

}
