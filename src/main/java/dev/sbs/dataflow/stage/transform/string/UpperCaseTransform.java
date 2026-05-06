package dev.sbs.dataflow.stage.transform.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that uppercases a {@link String} using
 * {@link String#toUpperCase()}.
 */
public final class UpperCaseTransform implements TransformStage<String, String> {

    /**
     * Constructs an uppercase stage.
     *
     * @return the stage
     */
    public static @NotNull UpperCaseTransform create() {
        return new UpperCaseTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_UPPERCASE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Uppercase"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : input.toUpperCase();
    }

}
