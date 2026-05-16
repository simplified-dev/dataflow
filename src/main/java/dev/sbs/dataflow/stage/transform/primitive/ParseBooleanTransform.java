package dev.sbs.dataflow.stage.transform.primitive;

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

/**
 * {@link TransformStage} that parses a {@link String} into a {@link Boolean}.
 * <p>
 * Recognises {@code "true"}/{@code "false"} (case-insensitive) plus {@code "1"} / {@code "0"}
 * and {@code "yes"} / {@code "no"}. Anything else returns {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_PARSE_BOOLEAN",
    displayName = "Parse boolean",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseBooleanTransform implements TransformStage<String, Boolean> {

    /**
     * Constructs a parse-boolean stage.
     *
     * @return the stage
     */
    public static @NotNull ParseBooleanTransform of() {
        return new ParseBooleanTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return switch (input.trim().toLowerCase()) {
            case "true", "1", "yes" -> Boolean.TRUE;
            case "false", "0", "no" -> Boolean.FALSE;
            default -> null;
        };
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse boolean";
    }

}
