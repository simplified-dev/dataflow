package dev.sbs.dataflow.stage.predicate.string;

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

import java.util.regex.Pattern;

/** {@link TransformStage} that returns {@code true} when the input matches the configured regex. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringMatchesPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a regex-matches predicate.
     *
     * @param regex the pattern that must {@link Pattern#matcher(CharSequence) find} a match in the input
     * @return the stage
     */
    public static @NotNull StringMatchesPredicate of(@NotNull String regex) {
        return new StringMatchesPredicate(regex, Pattern.compile(regex));
    }

    /**
     * Reconstructs a regex-matches predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull StringMatchesPredicate fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("regex"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("regex", this.regex)
            .build();
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
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_STRING_MATCHES;
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
