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
import dev.simplified.dataflow.stage.terminal.collect.JsonObjectFromEntriesCollect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link TransformStage} that turns each entry of a {@link JsonObject} into an object of its own,
 * so a payload keyed by id can be walked entry by entry.
 * <p>
 * It emits one new object per entry, in the object's order, which is document order. An entry
 * becomes {@code {keyField: key, valueField: value}}, the key a JSON string and the value the
 * entry's value as it is, so a JSON null value stays a JSON null. With the default field names it
 * is the inverse of {@link JsonObjectFromEntriesCollect}, which skips a null value, so a round
 * trip through both drops the null entries.
 * <p>
 * With {@link #inline} set, an object value's own fields are copied after {@code keyField}
 * instead of nested under {@code valueField}, and a field of the value named like
 * {@code keyField} gives way to the key. A value that is not an object keeps the
 * {@code valueField} form. The emitted objects hold the input's values rather than copies.
 */
@StageSpec(
    id = "TRANSFORM_JSON_ENTRIES",
    displayName = "JSON entries",
    description = "JSON_OBJECT -> List<JSON_OBJECT>",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntriesTransform implements TransformStage<JsonObject, List<JsonObject>> {

    /**
     * Name of the field carrying each entry's key when {@link #keyField} is absent.
     */
    public static final @NotNull String DEFAULT_KEY_FIELD = "key";

    /**
     * Name of the field carrying each entry's value when {@link #valueField} is absent.
     */
    public static final @NotNull String DEFAULT_VALUE_FIELD = "value";

    private static final @NotNull DataType<List<JsonObject>> OUTPUT = DataType.list(DataTypes.JSON_OBJECT);

    /**
     * Configured name of the field carrying each entry's key, or {@code null} for
     * {@value #DEFAULT_KEY_FIELD}.
     */
    private final @Nullable String keyField;

    /**
     * Configured name of the field carrying each entry's value, or {@code null} for
     * {@value #DEFAULT_VALUE_FIELD}.
     */
    private final @Nullable String valueField;

    /**
     * Configured choice to copy an object value's fields beside the key, or {@code null} for
     * {@code false}.
     */
    private final @Nullable Boolean inline;

    /**
     * Constructs a JSON-entries stage.
     *
     * @param keyField the field carrying each entry's key, or {@code null} for {@value #DEFAULT_KEY_FIELD}
     * @param valueField the field carrying each entry's value, or {@code null} for {@value #DEFAULT_VALUE_FIELD}
     * @param inline whether an object value's fields are copied beside the key, or {@code null} for {@code false}
     * @return the stage
     * @throws IllegalArgumentException when the key and value fields resolve to the same name
     */
    public static @NotNull EntriesTransform of(
        @Configurable(label = "Key field (optional)", placeholder = "key", optional = true)
        @Nullable String keyField,
        @Configurable(label = "Value field (optional)", placeholder = "value", optional = true)
        @Nullable String valueField,
        @Configurable(label = "Inline object values (optional)", placeholder = "false", optional = true)
        @Nullable Boolean inline
    ) {
        String keyName = keyName(keyField);

        if (keyName.equals(valueName(valueField)))
            throw new IllegalArgumentException(String.format("EntriesTransform key and value fields are both '%s'", keyName));

        return new EntriesTransform(keyField, valueField, inline);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable JsonObject input) {
        if (input == null) return null;
        String keyName = keyName(this.keyField);
        String valueName = valueName(this.valueField);
        boolean inlined = Boolean.TRUE.equals(this.inline);
        List<JsonObject> entries = new ArrayList<>(input.size());

        for (Map.Entry<String, JsonElement> entry : input.entrySet()) {
            JsonObject row = new JsonObject();
            row.addProperty(keyName, entry.getKey());
            JsonElement value = entry.getValue();

            if (inlined && value.isJsonObject())
                copyFields(value.getAsJsonObject(), row, keyName);
            else
                row.add(valueName, value);

            entries.add(row);
        }

        return Concurrent.newUnmodifiableList(entries);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> inputType() {
        return DataTypes.JSON_OBJECT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        String keyName = keyName(this.keyField);
        return Boolean.TRUE.equals(this.inline)
            ? "Entries as {" + keyName + ", ...fields}"
            : "Entries as {" + keyName + ", " + valueName(this.valueField) + "}";
    }

    /**
     * Copies every field of {@code value} into {@code row} except one named {@code keyName}, which
     * would replace the key.
     *
     * @param value the object value whose fields are copied
     * @param row the entry object receiving them
     * @param keyName the name of the field already carrying the key
     */
    private static void copyFields(@NotNull JsonObject value, @NotNull JsonObject row, @NotNull String keyName) {
        for (Map.Entry<String, JsonElement> field : value.entrySet()) {
            if (!field.getKey().equals(keyName))
                row.add(field.getKey(), field.getValue());
        }
    }

    private static @NotNull String keyName(@Nullable String keyField) {
        return keyField == null ? DEFAULT_KEY_FIELD : keyField;
    }

    private static @NotNull String valueName(@Nullable String valueField) {
        return valueField == null ? DEFAULT_VALUE_FIELD : valueField;
    }

}
