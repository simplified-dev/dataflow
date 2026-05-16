package dev.simplified.dataflow.stage.predicate.dom;

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
import org.jsoup.nodes.Element;

import java.util.regex.Pattern;

/**
 * {@link TransformStage} that returns {@code true} when the input element's
 * {@link Element#text()} matches the configured regex.
 */
@StageSpec(
    id = "PREDICATE_DOM_TEXT_MATCHES",
    displayName = "Text matches regex",
    description = "DOM_NODE -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTextMatchesPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a DOM-text-matches predicate.
     *
     * @param regex the regex pattern that must {@link Pattern#matcher(CharSequence) find} a match in the element's text
     * @return the stage
     */
    public static @NotNull DomTextMatchesPredicate of(
        @Configurable(label = "Text matches regex", placeholder = "\\d+")
        @NotNull @Language("regexp") String regex
    ) {
        return new DomTextMatchesPredicate(regex, Pattern.compile(regex));
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
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text matches '" + this.regex + "'";
    }

}
