package dev.sbs.dataflow.stage.transform.string;

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
 * {@link TransformStage} that returns the {@link String#length() character length} of the
 * input string.
 */
@StageSpec(
    id = "TRANSFORM_STRING_LENGTH",
    displayName = "String length",
    description = "STRING -> INT",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringLengthTransform implements TransformStage<String, Integer> {

    /**
     * Constructs a string-length stage.
     *
     * @return the stage
     */
    public static @NotNull StringLengthTransform of() {
        return new StringLengthTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : input.length();
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
        return "String length";
    }

}
