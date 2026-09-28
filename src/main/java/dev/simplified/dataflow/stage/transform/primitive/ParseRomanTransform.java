package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that reads a Roman numeral into an {@link Integer}, returning
 * {@code null} when the input is not one.
 * <p>
 * The input is trimmed and read in either case. Only a canonical numeral from {@code I} to
 * {@code MMMCMXCIX} (1 to 3999) is read: a repeated or misplaced symbol ({@code IIII},
 * {@code VX}, {@code IC}), an empty string or any other character yields {@code null}, as
 * {@link ParseIntTransform} does for a string it cannot parse.
 */
@StageSpec(
    id = "TRANSFORM_PARSE_ROMAN",
    displayName = "Parse Roman numeral",
    description = "STRING -> INT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseRomanTransform implements TransformStage<String, Integer> {

    /**
     * Canonical numerals from {@code I} to {@code MMMCMXCIX}, one group per decimal digit. It
     * also matches the empty string, which the stage refuses before matching.
     */
    private static final @NotNull Pattern CANONICAL = Pattern.compile(
        "M{0,3}(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{0,3})",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Constructs a parse-Roman stage.
     *
     * @return the stage
     */
    public static @NotNull ParseRomanTransform of() {
        return new ParseRomanTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        String numeral = input.trim();

        if (numeral.isEmpty() || !CANONICAL.matcher(numeral).matches())
            return null;

        int total = 0;

        for (int i = 0; i < numeral.length(); i++) {
            int value = valueOf(numeral.charAt(i));
            boolean subtractive = i + 1 < numeral.length() && value < valueOf(numeral.charAt(i + 1));
            total += subtractive ? -value : value;
        }

        return total;
    }

    private static int valueOf(char symbol) {
        return switch (Character.toUpperCase(symbol)) {
            case 'I' -> 1;
            case 'V' -> 5;
            case 'X' -> 10;
            case 'L' -> 50;
            case 'C' -> 100;
            case 'D' -> 500;
            case 'M' -> 1000;
            default -> throw new IllegalStateException("Symbol '" + symbol + "' passed the canonical numeral check");
        };
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
        return "Parse Roman numeral";
    }

}
