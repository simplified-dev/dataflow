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
 * {@link TransformStage} that returns {@link JsonElement#getAsBoolean()}, or {@code null}
 * when the element is not a primitive.
 */
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonAsBooleanTransform implements TransformStage<JsonElement, Boolean> {

    /**
     * Constructs a json-as-boolean stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsBooleanTransform of() {
        return new JsonAsBooleanTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.empty();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull() || !input.isJsonPrimitive()) return null;
        return input.getAsBoolean();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> inputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_JSON_AS_BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON as boolean";
    }

}
