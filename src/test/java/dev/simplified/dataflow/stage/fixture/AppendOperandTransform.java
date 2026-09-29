package dev.simplified.dataflow.stage.fixture;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.stage.FieldSpec;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Test-only {@link TransformStage} that appends the output of a {@code STRING} pipeline operand
 * to its input, exercising the {@link FieldSpec.Type#PIPELINE} slot from factory to wire.
 * <p>
 * Null in, null out, and a {@code null} operand output rejects the input with {@code null}.
 */
@StageSpec(
    id = "TEST_APPEND_OPERAND",
    displayName = "Append operand (test fixture)",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AppendOperandTransform implements TransformStage<String, String> {

    private final @NotNull DataPipeline<String> suffix;

    /**
     * Constructs a stage appending the output of {@code suffix} to every input.
     *
     * @param suffix the operand pipeline, whose output must be {@code STRING}
     * @return the stage
     * @throws IllegalArgumentException when {@code suffix} is invalid or does not produce {@code STRING}
     */
    @SuppressWarnings("unchecked")
    public static @NotNull AppendOperandTransform of(
        @Configurable(label = "Suffix pipeline")
        @NotNull DataPipeline<?> suffix
    ) {
        ValidationReport report = suffix.validate(DataTypes.STRING);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid AppendOperandTransform operand: " + report.issues());

        return new AppendOperandTransform((DataPipeline<String>) suffix);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        String tail = ctx.evaluateOperand(this.suffix);
        return tail == null ? null : input + tail;
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
    public @NotNull String summary() {
        return "Append operand (" + this.suffix.stages().size() + " stages)";
    }

}
