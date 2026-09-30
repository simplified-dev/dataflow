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
 * {@link TransformStage} that decodes the HTML character references in the input string as
 * jsoup's {@link Parser#unescapeEntities(String, boolean)} decodes text content.
 * <p>
 * It decodes named references such as {@code &amp;}, decimal ones such as {@code &#61;} and
 * hexadecimal ones such as {@code &#x7B;}. A name HTML does not define stays as written, and the
 * decoded text is not decoded a second time, so {@code &#38;#61;} becomes {@code &#61;}. Like a
 * parser reading text, it also decodes the legacy names HTML accepts without their closing
 * semicolon, such as {@code &amp}.
 * <p>
 * Where jsoup departs from the HTML standard's numeric references, and so from a browser, the
 * stage departs with it:
 * <ul>
 *   <li><b>{@code &#0;}</b> - decodes to U+0000, where the standard gives U+FFFD.</li>
 *   <li><b>A surrogate reference</b> such as {@code &#xD800;} - decodes to that lone surrogate,
 *       which Java's UTF-8 encoder writes as {@code ?}, and two in a row that form a pair decode
 *       to their one supplementary character, where the standard gives U+FFFD for each.</li>
 *   <li><b>A reference of more than about a thousand digits</b> - decodes only a leading run of
 *       its digits and leaves the rest as text, once it runs past a limit of between about one
 *       and two thousand characters that depends on where the reference falls in the input.</li>
 * </ul>
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
