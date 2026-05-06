package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * {@link TransformStage} that percent-decodes the input string using UTF-8.
 * Returns {@code null} when the input contains malformed escape sequences.
 */
public final class UrlDecodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a url-decode stage.
     *
     * @return the stage
     */
    public static @NotNull UrlDecodeTransform create() {
        return new UrlDecodeTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_URL_DECODE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "URL decode"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return URLDecoder.decode(input, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

}
