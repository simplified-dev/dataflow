package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link TransformStage} that expands a numeric range written in a {@link String} into the list
 * of its members.
 * <p>
 * The regex is searched for as {@link Matcher#find()} finds it, and its groups {@code 1} and
 * {@code 2} hold the low and the high bound, each parsed as {@code TRANSFORM_PARSE_INT} parses.
 * With the default regex, {@code "1-15"} becomes {@code [1, 2, ..., 15]} and {@code "-3--1"}
 * becomes {@code [-3, -2, -1]}. Members run from the low bound upward by the step, and the high
 * bound is a member only when the step lands on it. The input rejects with {@code null} when:
 * <ul>
 *   <li><b>the regex does not match</b>, or either bound group did not participate</li>
 *   <li><b>a bound is not an {@code int}</b></li>
 *   <li><b>the low bound is above the high bound</b></li>
 *   <li><b>the range has more members than {@code maxSize}</b> - a guard against a very large
 *       list</li>
 * </ul>
 */
@StageSpec(
    id = "TRANSFORM_RANGE_EXPAND",
    displayName = "Expand range",
    description = "STRING -> List<INT>",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RangeExpandTransform implements TransformStage<String, List<Integer>> {

    private static final @NotNull DataType<List<Integer>> OUTPUT = DataType.list(DataTypes.INT);

    private static final @NotNull @Language("regexp") String DEFAULT_REGEX = "^(-?\\d+)-(-?\\d+)$";

    private static final int DEFAULT_STEP = 1;

    private static final int DEFAULT_MAX_SIZE = 1000;

    /**
     * Pattern whose groups {@code 1} and {@code 2} hold the bounds, or {@code null} when not
     * configured, which is {@code ^(-?\d+)-(-?\d+)$}.
     */
    private final @Nullable String regex;

    /**
     * Distance between consecutive members, or {@code null} when not configured, which is {@code 1}.
     */
    private final @Nullable Integer step;

    /**
     * Largest member count expanded, or {@code null} when not configured, which is {@code 1000}.
     */
    private final @Nullable Integer maxSize;

    /**
     * The compiled {@code regex}, or the default pattern when none is configured.
     */
    private final @NotNull Pattern pattern;

    /**
     * Constructs a range-expand stage.
     *
     * @param regex the pattern whose groups {@code 1} and {@code 2} hold the bounds, or {@code null}
     *         for {@code ^(-?\d+)-(-?\d+)$}
     * @param step the distance between consecutive members, or {@code null} for {@code 1}
     * @param maxSize the largest member count expanded, or {@code null} for {@code 1000}
     * @return the stage
     * @throws IllegalArgumentException when {@code regex} does not compile or has fewer than two
     *         groups, or {@code step} or {@code maxSize} is below {@code 1}
     */
    public static @NotNull RangeExpandTransform of(
        @Configurable(label = "Regex (optional)", placeholder = DEFAULT_REGEX, optional = true)
        @Nullable @Language("regexp") String regex,
        @Configurable(label = "Step (optional)", placeholder = "1", optional = true)
        @Nullable Integer step,
        @Configurable(label = "Max size (optional)", placeholder = "1000", optional = true)
        @Nullable Integer maxSize
    ) {
        Pattern pattern = Pattern.compile(regex == null ? DEFAULT_REGEX : regex);

        if (pattern.matcher("").groupCount() < 2) {
            throw new IllegalArgumentException(String.format(
                "RangeExpandTransform regex '%s' needs two groups for the bounds", pattern.pattern()
            ));
        }

        if (step != null && step < 1)
            throw new IllegalArgumentException(String.format("RangeExpandTransform step '%s' is below 1", step));

        if (maxSize != null && maxSize < 1)
            throw new IllegalArgumentException(String.format("RangeExpandTransform maxSize '%s' is below 1", maxSize));

        return new RangeExpandTransform(regex, step, maxSize, pattern);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Integer> execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        Matcher matcher = this.pattern.matcher(input);
        if (!matcher.find()) return null;

        Integer low = bound(matcher.group(1));
        Integer high = bound(matcher.group(2));
        if (low == null || high == null || low > high) return null;

        int stride = this.step == null ? DEFAULT_STEP : this.step;
        int limit = this.maxSize == null ? DEFAULT_MAX_SIZE : this.maxSize;
        long count = ((long) high - low) / stride + 1;
        if (count > limit) return null;

        List<Integer> members = new ArrayList<>((int) count);
        for (long value = low; value <= high; value += stride)
            members.add((int) value);

        return Concurrent.newUnmodifiableList(members);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Expand range '" + this.pattern.pattern() + "'"
            + (this.step == null ? "" : " step " + this.step)
            + (this.maxSize == null ? "" : " max " + this.maxSize);
    }

    private static @Nullable Integer bound(@Nullable String text) {
        if (text == null) return null;

        try {
            return Integer.valueOf(text.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

}
