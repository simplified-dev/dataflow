package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that parses a {@link String} into a {@link Boolean}.
 * <p>
 * Recognises {@code "true"}/{@code "false"} (case-insensitive) plus {@code "1"} / {@code "0"}
 * and {@code "yes"} / {@code "no"}. Anything else returns {@code null}.
 */
public final class ParseBooleanTransform implements TransformStage<String, Boolean> {

    /**
     * Constructs a parse-boolean stage.
     *
     * @return the stage
     */
    public static @NotNull ParseBooleanTransform create() {
        return new ParseBooleanTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_PARSE_BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse boolean";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return switch (input.trim().toLowerCase()) {
            case "true", "1", "yes" -> Boolean.TRUE;
            case "false", "0", "no" -> Boolean.FALSE;
            default -> null;
        };
    }

}
