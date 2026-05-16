package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that serialises a {@link JsonElement} back into its JSON string
 * representation.
 */
@StageSpec(
    id = "TRANSFORM_JSON_STRINGIFY",
    displayName = "JSON stringify",
    description = "JSON_ELEMENT -> STRING",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringifyTransform implements TransformStage<JsonElement, String> {

    private static final @NotNull Gson GSON = new Gson();

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
