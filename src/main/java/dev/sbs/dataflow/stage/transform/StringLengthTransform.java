package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns the {@link String#length() character length} of the
 * input string.
 */
public final class StringLengthTransform implements TransformStage<String, Integer> {

    /**
     * Constructs a string-length stage.
     *
     * @return the stage
     */
    public static @NotNull StringLengthTransform create() {
        return new StringLengthTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()   { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<Integer> outputType() { return DataTypes.INT; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                 { return StageId.TRANSFORM_STRING_LENGTH; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()               { return "String length"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : input.length();
    }

}
