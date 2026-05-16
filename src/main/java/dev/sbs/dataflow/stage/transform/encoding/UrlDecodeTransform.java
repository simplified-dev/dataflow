package dev.sbs.dataflow.stage.transform.encoding;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * {@link TransformStage} that percent-decodes the input string with UTF-8. Returns
 * {@code null} when the input contains malformed escape sequences.
 */
@StageSpec(
    id = "TRANSFORM_URL_DECODE",
    displayName = "URL decode",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class UrlDecodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a url-decode stage.
     *
     * @return the stage
     */
    public static @NotNull UrlDecodeTransform of() {
        return new UrlDecodeTransform();
    }

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
        return "URL decode";
    }

}
