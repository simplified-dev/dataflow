package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * {@link TransformStage} that percent-encodes the input string using the
 * {@code application/x-www-form-urlencoded} convention with UTF-8.
 */
public final class UrlEncodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a url-encode stage.
     *
     * @return the stage
     */
    public static @NotNull UrlEncodeTransform create() {
        return new UrlEncodeTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_URL_ENCODE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "URL encode"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : URLEncoder.encode(input, StandardCharsets.UTF_8);
    }

}
