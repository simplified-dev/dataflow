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
 * {@link TransformStage} that returns {@link JsonElement#getAsBoolean()}, or {@code null}
 * when the element is not a primitive.
 */
@StageSpec(
    id = "TRANSFORM_JSON_AS_BOOLEAN",
    displayName = "JSON as boolean",
    description = "JSON_ELEMENT -> BOOLEAN",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsBooleanTransform implements TransformStage<JsonElement, Boolean> {

    /**
     * Constructs a json-as-boolean stage.
     *
     * @return the stage
     */
    public static @NotNull AsBooleanTransform of() {
        return new AsBooleanTransform();
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
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON as boolean";
    }

}
