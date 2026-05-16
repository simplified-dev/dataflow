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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * {@link TransformStage} that base64-decodes the input string, interpreting the bytes as
 * UTF-8. Returns {@code null} when the input is not valid base64.
 */
@StageSpec(
    id = "TRANSFORM_BASE64_DECODE",
    displayName = "Base64 decode",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Base64DecodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a base64-decode stage.
     *
     * @return the stage
     */
    public static @NotNull Base64DecodeTransform of() {
        return new Base64DecodeTransform();
    }

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
        return "Base64 decode";
    }

}
