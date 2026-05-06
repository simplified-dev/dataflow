package dev.sbs.dataflow.stage.transform;

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
 * {@link TransformStage} that base64-decodes the input and returns the bytes as a UTF-8
 * string. Returns {@code null} when the input is not valid base64.
 */
public final class Base64DecodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a base64-decode stage.
     *
     * @return the stage
     */
    public static @NotNull Base64DecodeTransform create() {
        return new Base64DecodeTransform();
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                { return StageId.TRANSFORM_BASE64_DECODE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Base64 decode"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return new String(Base64.getDecoder().decode(input), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

}
