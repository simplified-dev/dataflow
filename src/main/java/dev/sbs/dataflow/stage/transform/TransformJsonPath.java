package dev.sbs.dataflow.stage.transform;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that walks a dot-separated path through a Gson {@link JsonElement}
 * tree, returning the element at the path or {@code null} when any segment is missing or
 * not an object.
 * <p>
 * Path syntax mirrors the {@code @SerializedPath} annotation used in {@code gson-extras}:
 * dot-separated keys, no array indexing.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class TransformJsonPath implements TransformStage<JsonElement, JsonElement> {

    private final @NotNull String path;

    /**
     * Constructs a JSON-path walking stage.
     *
     * @param path dot-separated key path (e.g. {@code "stats.combat.dmg"})
     * @return the stage
     */
    public static @NotNull TransformJsonPath of(@NotNull String path) {
        return new TransformJsonPath(path);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> inputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_JSON_PATH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON path '" + this.path + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null) return null;
        JsonElement current = input;
        for (String segment : this.path.split("\\.")) {
            if (segment.isEmpty()) continue;
            if (!current.isJsonObject()) return null;
            current = current.getAsJsonObject().get(segment);
            if (current == null) return null;
        }
        return current;
    }

}
