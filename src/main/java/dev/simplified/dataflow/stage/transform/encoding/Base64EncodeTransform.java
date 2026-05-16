package dev.simplified.dataflow.stage.transform.encoding;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * {@link TransformStage} that base64-encodes the input string's UTF-8 bytes.
 */
@StageSpec(
    id = "TRANSFORM_BASE64_ENCODE",
    displayName = "Base64 encode",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_ENCODING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Base64EncodeTransform implements TransformStage<String, String> {

    /**
     * Constructs a base64-encode stage.
     *
     * @return the stage
     */
    public static @NotNull Base64EncodeTransform of() {
        return new Base64EncodeTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));
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
        return "Base64 encode";
    }

}
