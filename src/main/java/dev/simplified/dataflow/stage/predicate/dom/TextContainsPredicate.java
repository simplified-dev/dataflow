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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@code true} when the input element's
 * {@link Element#text()} contains the configured substring.
 */
@StageSpec(
    id = "PREDICATE_DOM_TEXT_CONTAINS",
    displayName = "Text contains",
    description = "DOM_NODE -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TextContainsPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String needle;

    /**
     * Constructs a DOM-text-contains predicate.
     *
     * @param needle the substring to look for in the element's text
     * @return the stage
     */
    public static @NotNull TextContainsPredicate of(
        @Configurable(label = "Text contains", placeholder = "Dmg")
        @NotNull String needle
    ) {
        return new TextContainsPredicate(needle);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input != null && input.text().contains(this.needle);
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
        return "Text contains '" + this.needle + "'";
    }

}
