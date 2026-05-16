package dev.sbs.dataflow.stage.transform.dom;

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
import org.jsoup.select.Elements;

/**
 * {@link TransformStage} that runs a child selector against a jsoup {@link Element} and
 * returns the {@code n}-th match, where {@code n} is 0-based. Returns {@code null} if the
 * index is out of range.
 */
@StageSpec(
    id = "TRANSFORM_NTH_CHILD",
    displayName = "Nth child",
    description = "DOM_NODE -> DOM_NODE",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NthChildTransform implements TransformStage<Element, Element> {

    private final @NotNull String childSelector;

    private final int index;

    /**
     * Constructs an n-th-child stage.
     *
     * @param childSelector jsoup CSS selector applied to children of the input
     * @param index zero-based index into the matches
     * @return the stage
     */
    public static @NotNull NthChildTransform of(
        @Configurable(label = "Child selector", placeholder = "td")
        @NotNull String childSelector,
        @Configurable(label = "Index (0-based)", placeholder = "0")
        int index
    ) {
        return new NthChildTransform(childSelector, index);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Element execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        Elements matches = input.select(this.childSelector);
        if (this.index < 0 || this.index >= matches.size()) return null;
        return matches.get(this.index);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> outputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Child #" + this.index + " '" + this.childSelector + "'";
    }

}
