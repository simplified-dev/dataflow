package dev.sbs.dataflow.stage.terminal.collect;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that merges a list of two-field entry objects into a single
 * {@link JsonObject}.
 * <p>
 * Each input element must be a {@code JsonObject} carrying two fields: a string-valued
 * {@code "key"} and an arbitrary {@code "value"}. Entries are merged in iteration order;
 * duplicate keys keep the last seen value. Entries missing either field, with a non-string
 * key, or whose value is {@link JsonElement#isJsonNull()} are skipped.
 */
@StageSpec(
    id = "COLLECT_JSON_OBJECT_FROM_ENTRIES",
    displayName = "JsonObject from entries",
    description = "List<JSON_OBJECT> -> JSON_OBJECT",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonObjectFromEntriesCollect implements CollectStage<List<JsonObject>, JsonObject> {

    private static final @NotNull JsonObjectFromEntriesCollect INSTANCE = new JsonObjectFromEntriesCollect();

    private static final @NotNull DataType<List<JsonObject>> INPUT = DataType.list(DataTypes.JSON_OBJECT);

    /**
     * Returns the singleton instance.
     *
     * @return the stage
     */
    public static @NotNull JsonObjectFromEntriesCollect of() {
        return INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonObject execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        JsonObject result = new JsonObject();
        for (JsonObject entry : input) {
            if (entry == null) continue;
            JsonElement keyEl = entry.get("key");
            JsonElement valueEl = entry.get("value");
            if (keyEl == null || valueEl == null) continue;
            if (!keyEl.isJsonPrimitive() || !keyEl.getAsJsonPrimitive().isString()) continue;
            if (valueEl.isJsonNull()) continue;
            result.add(keyEl.getAsString(), valueEl);
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> inputType() {
        return INPUT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> outputType() {
        return DataTypes.JSON_OBJECT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Merge {key,value} entries into JsonObject";
    }

}
