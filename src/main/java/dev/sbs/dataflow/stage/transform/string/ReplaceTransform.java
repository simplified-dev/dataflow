package dev.sbs.dataflow.stage.transform.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that replaces every regex match in the input with a replacement
 * string, equivalent to {@link String#replaceAll(String, String)}.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReplaceTransform implements TransformStage<String, String> {

    private final @NotNull String regex;

    private final @NotNull String replacement;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a replace stage.
     *
     * @param regex the pattern to match
     * @param replacement the replacement string (regex back-references allowed)
     * @return the stage
     */
    public static @NotNull ReplaceTransform of(@NotNull String regex, @NotNull String replacement) {
        return new ReplaceTransform(regex, replacement, Pattern.compile(regex));
    }

    /**
     * Reconstructs the transform from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull ReplaceTransform fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("regex"), cfg.getString("replacement"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("regex", this.regex)
            .string("replacement", this.replacement)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return this.pattern.matcher(input).replaceAll(this.replacement);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_REPLACE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Replace '" + this.regex + "' -> '" + this.replacement + "'";
    }

}
