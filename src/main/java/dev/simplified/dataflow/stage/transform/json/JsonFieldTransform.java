package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns a single named field of a {@link JsonObject}.
 * Returns {@code null} when the field is absent.
 */
@StageSpec(
    id = "TRANSFORM_JSON_FIELD",
    displayName = "JSON field",
    description = "JSON_OBJECT -> JSON_ELEMENT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonFieldTransform implements TransformStage<JsonObject, JsonElement> {

    private final @NotNull String fieldName;

    /**
     * Constructs a JSON-field stage.
     *
     * @param fieldName the field to extract
     * @return the stage
     */
    public static @NotNull JsonFieldTransform of(
        @Configurable(label = "Field name", placeholder = "stats")
        @NotNull String fieldName
    ) {
        return new JsonFieldTransform(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable JsonObject input) {
        if (input == null) return null;
        return input.get(this.fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> inputType() {
        return DataTypes.JSON_OBJECT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Field '" + this.fieldName + "'";
    }

}
