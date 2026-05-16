package dev.sbs.dataflow.stage.predicate.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
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
public final class DomTextContainsPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String needle;

    /**
     * Constructs a node-text-contains predicate.
     *
     * @param needle the substring to look for in the element's text
     * @return the stage
     */
    public static @NotNull DomTextContainsPredicate of(
        @Configurable(label = "Text contains", placeholder = "Dmg")
        @NotNull String needle
    ) {
        return new DomTextContainsPredicate(needle);
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
