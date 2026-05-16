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
 * {@link TransformStage} that converts an input {@link String} to {@code camelCase}.
 * <p>
 * Words are detected at whitespace, {@code _}, and {@code -} separators, plus at
 * lower-to-upper transitions and acronym-to-capitalised-word transitions (so
 * {@code XMLParser} splits into {@code [XML, Parser]}). The first word is lowercased;
 * subsequent words are lowercased then have their first character capitalised.
 * Separators are dropped.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code "hello world"} -&gt; {@code "helloWorld"}</li>
 *   <li>{@code "hello_world"} -&gt; {@code "helloWorld"}</li>
 *   <li>{@code "HelloWorld"} -&gt; {@code "helloWorld"}</li>
 *   <li>{@code "XMLParser"} -&gt; {@code "xmlParser"}</li>
 * </ul>
 */
@StageSpec(
    id = "TRANSFORM_CAMEL_CASE",
    displayName = "Camel case",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CamelCaseTransform implements TransformStage<String, String> {

    private static final @NotNull Pattern WORD_BOUNDARY = Pattern.compile(
        "[\\s_-]+|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])"
    );

    /**
     * Constructs a camel-case stage.
     *
     * @return the stage
     */
    public static @NotNull CamelCaseTransform of() {
        return new CamelCaseTransform();
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
            String lower = word.toLowerCase();
            if (first) {
                result.append(lower);
                first = false;
            } else {
                result.append(Character.toUpperCase(lower.charAt(0))).append(lower, 1, lower.length());
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
        return "camelCase";
    }

}
