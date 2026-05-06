package dev.sbs.dataflow.stage.transform.encoding;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * {@link TransformStage} that base64-encodes the UTF-8 bytes of the input string.
 */
public final class Base64EncodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a base64-encode stage.
     *
     * @return the stage
     */
    public static @NotNull Base64EncodeTransform create() {
        return new Base64EncodeTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_BASE64_ENCODE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Base64 encode"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));
    }

}
