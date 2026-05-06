package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that replaces every regex match in the input with a replacement
 * string, equivalent to {@link String#replaceAll(String, String)}.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class ReplaceTransform implements TransformStage<String, String> {

    private final @NotNull String regex;
    private final @NotNull String replacement;

    /**
     * Constructs a replace stage.
     *
     * @param regex the pattern to match
     * @param replacement the replacement string (regex back-references allowed)
     * @return the stage
     */
    public static @NotNull ReplaceTransform of(@NotNull String regex, @NotNull String replacement) {
        return new ReplaceTransform(regex, replacement);
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
        return StageId.TRANSFORM_REPLACE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Replace '" + this.regex + "' -> '" + this.replacement + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return input.replaceAll(this.regex, this.replacement);
    }

}
