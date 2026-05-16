package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_JSON} body into a Gson
 * {@link JsonElement}.
 */
@StageSpec(
    id = "PARSE_JSON",
    displayName = "Parse JSON",
    description = "RAW_JSON -> JSON_ELEMENT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseJsonTransform implements TransformStage<String, JsonElement> {

    /**
     * Constructs a parse-JSON stage.
     *
     * @return the stage
     */
    public static @NotNull ParseJsonTransform of() {
        return new ParseJsonTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return JsonParser.parseString(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_JSON;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as JSON";
    }

}
