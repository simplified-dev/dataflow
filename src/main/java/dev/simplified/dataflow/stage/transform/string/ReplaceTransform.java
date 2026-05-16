package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that replaces every regex match in the input with a replacement
 * string, equivalent to {@link String#replaceAll(String, String)}.
 */
@StageSpec(
    id = "TRANSFORM_REPLACE",
    displayName = "Replace regex",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
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
    public static @NotNull ReplaceTransform of(
        @Configurable(label = "Regex", placeholder = "\\s+")
        @NotNull @Language("regexp") String regex,
        @Configurable(label = "Replacement", placeholder = "_")
        @NotNull String replacement
    ) {
        return new ReplaceTransform(regex, replacement, Pattern.compile(regex));
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
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Replace '" + this.regex + "' -> '" + this.replacement + "'";
    }

}
