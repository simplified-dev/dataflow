package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that converts an input {@link String} to {@code Title Case}.
 * <p>
 * Words are detected at whitespace boundaries only; all other characters
 * (punctuation, digits, hyphens) are preserved in their position and do not reset
 * the word-start flag. The first character of each word is uppercased; every other
 * character is lowercased. Whitespace is preserved verbatim.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code "hello world"} -&gt; {@code "Hello World"}</li>
 *   <li>{@code "HELLO WORLD"} -&gt; {@code "Hello World"}</li>
 *   <li>{@code "  hello  world  "} -&gt; {@code "  Hello  World  "}</li>
 *   <li>{@code "self-reliance"} -&gt; {@code "Self-reliance"} (no hyphen-aware split)</li>
 * </ul>
 */
@StageSpec(
    id = "TRANSFORM_TITLE_CASE",
    displayName = "Title case",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TitleCaseTransform implements TransformStage<String, String> {

    /**
     * Constructs a title-case stage.
     *
     * @return the stage
     */
    public static @NotNull TitleCaseTransform of() {
        return new TitleCaseTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        StringBuilder result = new StringBuilder(input.length());
        boolean atWordStart = true;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) {
                result.append(c);
                atWordStart = true;
            } else if (atWordStart) {
                result.append(Character.toUpperCase(c));
                atWordStart = false;
            } else {
                result.append(Character.toLowerCase(c));
            }
        }
        return result.toString();
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
        return "Title Case";
    }

}
