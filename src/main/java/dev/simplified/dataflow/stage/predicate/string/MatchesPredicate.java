package dev.simplified.dataflow.stage.predicate.string;

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
 * {@link TransformStage} that returns {@code true} when the input matches the configured regex.
 */
@StageSpec(
    id = "PREDICATE_STRING_MATCHES",
    displayName = "Matches regex",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MatchesPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a regex-matches predicate.
     *
     * @param regex the pattern that must {@link Pattern#matcher(CharSequence) find} a match in the input
     * @return the stage
     */
    public static @NotNull MatchesPredicate of(
        @Configurable(label = "Regex", placeholder = "^foo")
        @NotNull @Language("regexp") String regex
    ) {
        return new MatchesPredicate(regex, Pattern.compile(regex));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input != null && this.pattern.matcher(input).find();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Matches '" + this.regex + "'";
    }

}
