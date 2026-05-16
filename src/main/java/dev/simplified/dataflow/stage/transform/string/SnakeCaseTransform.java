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

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that converts an input {@link String} to {@code snake_case}.
 * <p>
 * Word detection matches {@link CamelCaseTransform}: whitespace, {@code _}, {@code -},
 * lower-to-upper transitions, and acronym-to-capitalised-word transitions all split
 * words. Every word is lowercased and joined with a single {@code _}; existing
 * separators are dropped.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code "hello world"} -&gt; {@code "hello_world"}</li>
 *   <li>{@code "HelloWorld"} -&gt; {@code "hello_world"}</li>
 *   <li>{@code "hello-world"} -&gt; {@code "hello_world"}</li>
 *   <li>{@code "XMLParser"} -&gt; {@code "xml_parser"}</li>
 * </ul>
 */
@StageSpec(
    id = "TRANSFORM_SNAKE_CASE",
    displayName = "Snake case",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SnakeCaseTransform implements TransformStage<String, String> {

    private static final @NotNull Pattern WORD_BOUNDARY = Pattern.compile(
        "[\\s_-]+|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])"
    );

    /**
     * Constructs a snake-case stage.
     *
     * @return the stage
     */
    public static @NotNull SnakeCaseTransform of() {
        return new SnakeCaseTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        String[] words = WORD_BOUNDARY.split(input);
        StringBuilder result = new StringBuilder(input.length());
        boolean first = true;
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (!first) result.append('_');
            result.append(word.toLowerCase());
            first = false;
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
        return "snake_case";
    }

}
