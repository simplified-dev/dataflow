package dev.simplified.dataflow.stage.transform.encoding;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that decodes the JSON string escapes in the input string.
 * <p>
 * It decodes {@code \"}, {@code \\}, {@code \/}, {@code \b}, {@code \f}, {@code \n},
 * {@code \r}, {@code \t} and <code>&#92;u</code> followed by four hex digits. Two
 * <code>&#92;u</code> escapes that encode a surrogate pair decode to the one supplementary
 * character they form, and a lone surrogate decodes to itself, as a JSON parser reads it. Every
 * other character passes through unchanged, including a quote nothing escapes.
 * <p>
 * Returns {@code null} when the input holds a malformed escape rather than guessing its meaning:
 * a backslash that ends the input, a backslash followed by a character no JSON escape starts
 * with, or a <code>&#92;u</code> followed by fewer than four hex digits.
 */
@StageSpec(
    id = "TRANSFORM_JSON_UNESCAPE",
    displayName = "JSON unescape",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonUnescapeTransform implements TransformStage<String, String> {

    /**
     * Constructs a JSON-unescape stage.
     *
     * @return the stage
     */
    public static @NotNull JsonUnescapeTransform of() {
        return new JsonUnescapeTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        if (input.indexOf('\\') < 0) return input;

        StringBuilder decoded = new StringBuilder(input.length());
        int length = input.length();
        int index = 0;

        while (index < length) {
            char current = input.charAt(index++);

            if (current != '\\') {
                decoded.append(current);
                continue;
            }

            if (index == length) return null;
            char escape = input.charAt(index++);

            switch (escape) {
                case '"', '\\', '/' -> decoded.append(escape);
                case 'b' -> decoded.append('\b');
                case 'f' -> decoded.append('\f');
                case 'n' -> decoded.append('\n');
                case 'r' -> decoded.append('\r');
                case 't' -> decoded.append('\t');
                case 'u' -> {
                    int unit = unit(input, index);
                    if (unit < 0) return null;
                    decoded.append((char) unit);
                    index += 4;
                }
                default -> {
                    return null;
                }
            }
        }

        return decoded.toString();
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
        return "JSON unescape";
    }

    /**
     * Reads the UTF-16 code unit spelled by the four hex digits starting at {@code from}.
     *
     * @param input the string being decoded
     * @param from the index of the first hex digit
     * @return the code unit, or {@code -1} when fewer than four hex digits follow
     */
    private static int unit(@NotNull String input, int from) {
        if (from + 4 > input.length()) return -1;
        int unit = 0;

        for (int index = from; index < from + 4; index++) {
            int digit = hexDigit(input.charAt(index));
            if (digit < 0) return -1;
            unit = (unit << 4) | digit;
        }

        return unit;
    }

    /**
     * Reads one ASCII hex digit. Unlike {@link Character#digit(char, int)}, it accepts no
     * non-ASCII digit, since JSON admits none.
     *
     * @param c the character
     * @return the digit's value, or {@code -1} when {@code c} is not an ASCII hex digit
     */
    private static int hexDigit(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

}
