package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
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
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_JSON_FIELD;
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
