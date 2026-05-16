package dev.sbs.dataflow.stage.transform.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_HTML} body into a jsoup
 * {@link Element} suitable for CSS-selector traversal.
 */
@StageSpec(
    displayName = "Parse HTML",
    description = "RAW_HTML -> DOM_NODE",
    category = StageSpec.Category.TRANSFORM_DOM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseHtmlTransform implements TransformStage<String, Element> {

    /**
     * Constructs a parse-HTML stage.
     *
     * @return the stage
     */
    public static @NotNull ParseHtmlTransform of() {
        return new ParseHtmlTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Element execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return Jsoup.parse(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_HTML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PARSE_HTML;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> outputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as HTML";
    }

}
