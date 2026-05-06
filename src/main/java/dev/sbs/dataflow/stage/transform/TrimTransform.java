package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that strips leading and trailing whitespace from a {@link String}.
 */
public final class TrimTransform implements TransformStage<String, String> {

    /**
     * Constructs a trim stage.
     *
     * @return the stage
     */
    public static @NotNull TrimTransform create() {
        return new TrimTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_TRIM;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Trim";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return input.trim();
    }

}
