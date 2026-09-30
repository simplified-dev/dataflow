package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that serialises a {@link JsonElement} back into its compact JSON string
 * representation, which parses back to the same element.
 * <p>
 * Serialises with the settings of {@link PipelineGson#gson()}, which does not HTML-escape, so
 * {@code <}, {@code >}, {@code &}, {@code =} and {@code '} inside strings come out as themselves
 * rather than as Unicode escape sequences. An object member whose value is JSON {@code null} is
 * written as {@code null} at any depth rather than left out.
 */
@StageSpec(
    id = "TRANSFORM_JSON_STRINGIFY",
    displayName = "JSON stringify",
    description = "JSON_ELEMENT -> STRING",
    category = StageSpec.Category.TRANSFORM_JSON
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringifyTransform implements TransformStage<JsonElement, String> {

    /**
     * {@link PipelineGson#gson()} writing null object members.
     */
    private static final @NotNull Gson GSON = PipelineGson.gson().newBuilder().serializeNulls().create();

    /**
     * Constructs a json-stringify stage.
     *
     * @return the stage
     */
    public static @NotNull StringifyTransform of() {
        return new StringifyTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        return input == null ? null : GSON.toJson(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> inputType() {
        return DataTypes.JSON_ELEMENT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON stringify";
    }

}
