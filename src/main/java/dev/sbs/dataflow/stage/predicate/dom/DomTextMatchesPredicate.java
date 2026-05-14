package dev.sbs.dataflow.stage.predicate.dom;

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
import org.jsoup.nodes.Element;

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that returns {@code true} when the input element's
 * {@link Element#text()} matches the configured regex.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTextMatchesPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a node-text-matches predicate.
     *
     * @param regex the regex pattern that must {@link Pattern#matcher(CharSequence) find} a match in the element's text
     * @return the stage
     */
    public static @NotNull DomTextMatchesPredicate of(@NotNull String regex) {
        return new DomTextMatchesPredicate(regex, Pattern.compile(regex));
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull DomTextMatchesPredicate fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input != null && this.pattern.matcher(input.text()).find();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_DOM_TEXT_MATCHES;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text matches '" + this.regex + "'";
    }

}
