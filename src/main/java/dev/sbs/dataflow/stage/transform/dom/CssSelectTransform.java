package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.util.List;

/**
 * {@link TransformStage} that runs a jsoup CSS selector against a {@link Element} and returns
 * every matching element as a {@code List<Element>}.
 */
@StageSpec(
    id = "TRANSFORM_CSS_SELECT",
    displayName = "CSS select",
    description = "DOM_NODE -> List<DOM_NODE>",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class CssSelectTransform implements TransformStage<Element, List<Element>> {

    private static final @NotNull DataType<List<Element>> OUTPUT = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String selector;

    /**
     * Constructs a CSS-select stage.
     *
     * @param selector the jsoup-flavoured CSS selector
     * @return the stage
     */
    public static @NotNull CssSelectTransform of(
        @Configurable(label = "Selector", placeholder = "table.infobox tr")
        @NotNull String selector
    ) {
        return new CssSelectTransform(selector);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Element> execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        return Concurrent.newUnmodifiableList(input.select(this.selector));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Element>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "CSS select '" + this.selector + "'";
    }

}
