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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link TransformStage} that runs a regex against the input and returns the requested
 * capture group of the first match. Returns {@code null} when no match is found.
 * <p>
 * Group {@code 0} (the entire match) is the default; positive group indexes refer to
 * parenthesised capture groups.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RegexExtractTransform implements TransformStage<String, String> {

    private final @NotNull String regex;

    private final int group;

    /**
     * Constructs a regex-extract stage that returns the entire match.
     *
     * @param regex the pattern
     * @return the stage
     */
    public static @NotNull RegexExtractTransform of(@NotNull String regex) {
        return new RegexExtractTransform(regex, 0);
    }

    /**
     * Constructs a regex-extract stage that returns the given capture group.
     *
     * @param regex the pattern
     * @param group the 0-based group index
     * @return the stage
     */
    public static @NotNull RegexExtractTransform of(@NotNull String regex, int group) {
        return new RegexExtractTransform(regex, group);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("regex", this.regex)
            .integer("group", this.group)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        Matcher m = Pattern.compile(this.regex).matcher(input);
        if (!m.find()) return null;
        if (this.group < 0 || this.group > m.groupCount()) return null;
        return m.group(this.group);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_REGEX_EXTRACT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Regex '" + this.regex + "'" + (this.group == 0 ? "" : " group " + this.group);
    }

}
