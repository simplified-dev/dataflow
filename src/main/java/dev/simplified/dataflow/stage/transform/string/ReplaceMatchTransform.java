package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link TransformStage} that rewrites the matched parts of a {@link String} through a
 * sub-pipeline body, leaving the text around them untouched.
 * <p>
 * The regex is matched left to right as {@link Matcher#find()} finds it. For each match, the
 * text of the configured capture group runs through the body and the body's result replaces
 * that group in place; the rest of the match and the text between matches are copied as they
 * are. The body's result is inserted literally, so {@code $} and {@code \} carry no meaning in
 * it. A match is left unchanged when:
 * <ul>
 *   <li><b>the body yields {@code null}</b> - so a body can rewrite only the matches it
 *       accepts</li>
 *   <li><b>the group did not participate</b> - an alternative the match did not take</li>
 *   <li><b>the group overlaps a replaced part</b> - possible only for a group inside a
 *       lookaround, which can reach past its own match</li>
 * </ul>
 * With group {@code 1} of {@code %\{KEYWORD:(.+?)\}} and a body of {@link SnakeCaseTransform}
 * then {@link UpperCaseTransform}, {@code "%{KEYWORD:Heart of the Mountain}"} becomes
 * {@code "%{KEYWORD:HEART_OF_THE_MOUNTAIN}"}.
 */
@StageSpec(
    id = "TRANSFORM_REPLACE_MATCH",
    displayName = "Replace match through body",
    description = "STRING -> STRING (body: STRING -> STRING)",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReplaceMatchTransform implements TransformStage<String, String> {

    /**
     * The pattern whose matches are rewritten.
     */
    private final @NotNull String regex;

    /**
     * Capture group rewritten in each match, or {@code null} when not configured, which is group {@code 0}.
     */
    private final @Nullable Integer group;

    /**
     * Sub-pipeline each matched group's text runs through.
     */
    private final @NotNull Chain<String, String> body;

    /**
     * The compiled {@code regex}.
     */
    private final @NotNull Pattern pattern;

    /**
     * Constructs a replace-match stage.
     *
     * @param regex the pattern whose matches are rewritten
     * @param group the capture group rewritten in each match, or {@code null} for the whole match
     * @param body the sub-pipeline each group's text runs through, consuming and producing {@code STRING}
     * @return the stage
     * @throws IllegalArgumentException when {@code regex} does not compile, {@code group} is negative
     *         or past the pattern's group count, or {@code body} fails type-chain validation
     */
    public static @NotNull ReplaceMatchTransform of(
        @Configurable(label = "Regex", placeholder = "%\\{KEYWORD:([^}]+)\\}")
        @NotNull @Language("regexp") String regex,
        @Configurable(label = "Capture group (optional)", placeholder = "1", optional = true)
        @Nullable Integer group,
        @Configurable(label = "Per-match body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        Pattern pattern = Pattern.compile(regex);
        int groupCount = pattern.matcher("").groupCount();

        if (group != null && group < 0)
            throw new IllegalArgumentException(String.format("ReplaceMatchTransform group '%s' is negative", group));

        if (group != null && group > groupCount) {
            throw new IllegalArgumentException(String.format(
                "ReplaceMatchTransform group '%s' is past the pattern's group count '%s'", group, groupCount
            ));
        }

        ValidationReport report = Chain.validate(DataTypes.STRING, body, DataTypes.STRING);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid replaceMatch body: " + report.issues());

        return new ReplaceMatchTransform(regex, group, Chain.of(body), pattern);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        int index = this.group == null ? 0 : this.group;
        Matcher matcher = this.pattern.matcher(input);
        StringBuilder output = null;
        int copied = 0;

        while (matcher.find()) {
            int start = matcher.start(index);
            if (start < copied) continue;

            String replaced = this.body.execute(ctx, matcher.group(index));
            if (replaced == null) continue;

            if (output == null) output = new StringBuilder(input.length());
            output.append(input, copied, start).append(replaced);
            copied = matcher.end(index);
        }

        if (output == null) return input;
        return output.append(input, copied, input.length()).toString();
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
        return "Replace match '" + this.regex + "'"
            + (this.group == null || this.group == 0 ? "" : " group " + this.group)
            + " (" + this.body.size() + " stage" + (this.body.size() == 1 ? "" : "s") + ")";
    }

}
