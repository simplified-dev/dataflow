package dev.sbs.dataflow.stage.transform.encoding;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * {@link TransformStage} that percent-encodes the input string with UTF-8 using the
 * {@code application/x-www-form-urlencoded} convention.
 */
@StageSpec(
    id = "TRANSFORM_URL_ENCODE",
    displayName = "URL encode",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class UrlEncodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a url-encode stage.
     *
     * @return the stage
     */
    public static @NotNull UrlEncodeTransform of() {
        return new UrlEncodeTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : URLEncoder.encode(input, StandardCharsets.UTF_8);
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
        return "URL encode";
    }

}
