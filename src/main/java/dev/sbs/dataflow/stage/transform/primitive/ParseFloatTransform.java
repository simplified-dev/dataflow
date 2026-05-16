package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link String} into a {@link Float}, returning
 * {@code null} when the input is unparseable.
 */
@StageSpec(
    id = "TRANSFORM_PARSE_FLOAT",
    displayName = "Parse float",
    description = "STRING -> FLOAT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseFloatTransform implements TransformStage<String, Float> {

    /**
     * Constructs a parse-float stage.
     *
     * @return the stage
     */
    public static @NotNull ParseFloatTransform of() {
        return new ParseFloatTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return Float.valueOf(input.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Float> outputType() {
        return DataTypes.FLOAT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse float";
    }

}
