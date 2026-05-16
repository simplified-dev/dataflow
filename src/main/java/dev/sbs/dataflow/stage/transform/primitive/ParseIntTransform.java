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
 * {@link TransformStage} that parses a {@link String} into an {@link Integer}, returning
 * {@code null} when the input is unparseable.
 */
@StageSpec(
    id = "TRANSFORM_PARSE_INT",
    displayName = "Parse int",
    description = "STRING -> INT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseIntTransform implements TransformStage<String, Integer> {

    /**
     * Constructs a parse-int stage.
     *
     * @return the stage
     */
    public static @NotNull ParseIntTransform of() {
        return new ParseIntTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return Integer.valueOf(input.trim());
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
    public @NotNull DataType<Integer> outputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse int";
    }

}
