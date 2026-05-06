package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link String} into an {@link Integer}, returning
 * {@code null} when the input is unparseable.
 */
public final class TransformParseInt implements TransformStage<String, Integer> {

    /**
     * Constructs a parse-int stage.
     *
     * @return the stage
     */
    public static @NotNull TransformParseInt create() {
        return new TransformParseInt();
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
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_PARSE_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse int";
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

}
