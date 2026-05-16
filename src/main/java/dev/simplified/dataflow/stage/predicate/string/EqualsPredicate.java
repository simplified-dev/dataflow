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
 * {@link TransformStage} that returns {@code true} when the input is exactly equal to the configured target.
 */
@StageSpec(
    id = "PREDICATE_STRING_EQUALS",
    displayName = "Equals",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class EqualsPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String target;

    /**
     * Constructs an equals predicate.
     *
     * @param target the exact string to match
     * @return the stage
     */
    public static @NotNull EqualsPredicate of(
        @Configurable(label = "Equals", placeholder = "foo")
        @NotNull String target
    ) {
        return new EqualsPredicate(target);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return this.target.equals(input);
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
        return "Equals '" + this.target + "'";
    }

}
