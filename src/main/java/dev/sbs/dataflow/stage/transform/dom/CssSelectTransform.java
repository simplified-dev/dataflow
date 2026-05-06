package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
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
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class CssSelectTransform implements TransformStage<Element, List<Element>> {

    private static final @NotNull DataType<List<Element>> OUTPUT = DataType.list(DataTypes.DOM_NODE);

    private final @NotNull String selector;

    /**
     * Constructs a CSS-select stage.
     *
     * @param selector the jsoup-flavoured CSS selector
     * @return the stage
     */
    public static @NotNull CssSelectTransform of(@NotNull String selector) {
        return new CssSelectTransform(selector);
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
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_CSS_SELECT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "CSS select '" + this.selector + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Element> execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return null;
        return List.copyOf(input.select(this.selector));
    }

}
