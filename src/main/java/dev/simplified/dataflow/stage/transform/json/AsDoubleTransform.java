package dev.simplified.dataflow.stage.transform.json;

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
 * {@link TransformStage} that returns {@link JsonElement#getAsDouble()}, or {@code null}
 * when the element is not a numeric primitive.
 */
@StageSpec(
    id = "TRANSFORM_JSON_AS_DOUBLE",
    displayName = "JSON as double",
    description = "JSON_ELEMENT -> DOUBLE",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsDoubleTransform implements TransformStage<JsonElement, Double> {

    /**
     * Constructs a json-as-double stage.
     *
     * @return the stage
     */
    public static @NotNull AsDoubleTransform of() {
        return new AsDoubleTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull() || !input.isJsonPrimitive()) return null;
        try {
            return input.getAsDouble();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> inputType() {
        return DataTypes.JSON_ELEMENT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON as double";
    }

}
