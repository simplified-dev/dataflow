package dev.simplified.dataflow.stage.predicate.string;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@code true} when the input ends with the configured suffix.
 */
@StageSpec(
    id = "PREDICATE_STRING_ENDS_WITH",
    displayName = "Ends with",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringEndsWithPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String suffix;

    /**
     * Constructs an ends-with predicate.
     *
     * @param suffix the suffix to require
     * @return the stage
     */
    public static @NotNull StringEndsWithPredicate of(
        @Configurable(label = "Suffix", placeholder = "bar")
        @NotNull String suffix
    ) {
        return new StringEndsWithPredicate(suffix);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input != null && input.endsWith(this.suffix);
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
        return "Ends with '" + this.suffix + "'";
    }

}
