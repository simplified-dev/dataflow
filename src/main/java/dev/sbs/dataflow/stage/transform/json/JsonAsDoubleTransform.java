package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
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
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonAsDoubleTransform implements TransformStage<JsonElement, Double> {

    /**
     * Constructs a json-as-double stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsDoubleTransform of() {
        return new JsonAsDoubleTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.empty();
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
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_JSON_AS_DOUBLE;
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
