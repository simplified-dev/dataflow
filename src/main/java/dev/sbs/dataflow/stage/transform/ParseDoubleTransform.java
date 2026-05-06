package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link String} into a {@link Double}, returning
 * {@code null} when the input is unparseable.
 */
public final class ParseDoubleTransform implements TransformStage<String, Double> {

    /**
     * Constructs a parse-double stage.
     *
     * @return the stage
     */
    public static @NotNull ParseDoubleTransform create() {
        return new ParseDoubleTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_PARSE_DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse double";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return Double.valueOf(input.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

}
