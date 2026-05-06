package dev.sbs.dataflow.stage.transform.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that lowercases a {@link String} using
 * {@link String#toLowerCase()}.
 */
public final class LowerCaseTransform implements TransformStage<String, String> {

    /**
     * Constructs a lowercase stage.
     *
     * @return the stage
     */
    public static @NotNull LowerCaseTransform create() {
        return new LowerCaseTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_LOWERCASE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Lowercase"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : input.toLowerCase();
    }

}
