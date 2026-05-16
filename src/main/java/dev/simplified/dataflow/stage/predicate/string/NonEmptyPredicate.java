package dev.simplified.dataflow.stage.predicate.string;

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

/**
 * {@link TransformStage} that returns {@code true} when the input is neither {@code null} nor empty.
 */
@StageSpec(
    id = "PREDICATE_STRING_NON_EMPTY",
    displayName = "Non-empty",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NonEmptyPredicate implements TransformStage<String, Boolean> {

    /**
     * Constructs a non-empty-string predicate.
     *
     * @return the stage
     */
    public static @NotNull NonEmptyPredicate of() {
        return new NonEmptyPredicate();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input != null && !input.isEmpty();
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
        return "Non-empty string";
    }

}
