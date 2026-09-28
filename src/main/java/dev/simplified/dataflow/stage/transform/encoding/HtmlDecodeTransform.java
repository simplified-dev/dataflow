package dev.simplified.dataflow.stage.transform.encoding;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.parser.Parser;

/**
 * {@link TransformStage} that decodes the HTML character references in the input string, as an
 * HTML parser decodes text content.
 * <p>
 * It decodes named references such as {@code &amp;}, decimal ones such as {@code &#61;} and
 * hexadecimal ones such as {@code &#x7B;}, with jsoup's {@link Parser#unescapeEntities(String, boolean)}.
 * A name HTML does not define stays as written, and the decoded text is not decoded a second
 * time, so {@code &#38;#61;} becomes {@code &#61;}. Like a parser reading text, it also decodes
 * the legacy names HTML accepts without their closing semicolon, such as {@code &amp}.
 */
@StageSpec(
    id = "TRANSFORM_HTML_DECODE",
    displayName = "HTML decode",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HtmlDecodeTransform implements TransformStage<String, String> {

    /**
     * Constructs an HTML-decode stage.
     *
     * @return the stage
     */
    public static @NotNull HtmlDecodeTransform of() {
        return new HtmlDecodeTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return Parser.unescapeEntities(input, false);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "HTML decode";
    }

}
