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
 * {@link TransformStage} that returns {@link JsonElement#getAsString()}, or {@code null}
 * when the element is not a primitive.
 */
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonAsStringTransform implements TransformStage<JsonElement, String> {

    /**
     * Constructs a json-as-string stage.
     *
     * @return the stage
     */
    public static @NotNull JsonAsStringTransform of() {
        return new JsonAsStringTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.empty();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable JsonElement input) {
        if (input == null || input.isJsonNull()) return null;
        if (!input.isJsonPrimitive()) return null;
        return input.getAsString();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> inputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_JSON_AS_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JSON as string";
    }

}
