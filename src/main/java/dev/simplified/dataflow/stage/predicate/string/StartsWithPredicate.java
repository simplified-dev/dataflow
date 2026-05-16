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
 * {@link TransformStage} that returns {@code true} when the input starts with the configured prefix.
 */
@StageSpec(
    id = "PREDICATE_STRING_STARTS_WITH",
    displayName = "Starts with",
    description = "STRING -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StartsWithPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String prefix;

    /**
     * Constructs a starts-with predicate.
     *
     * @param prefix the prefix to require
     * @return the stage
     */
    public static @NotNull StartsWithPredicate of(
        @Configurable(label = "Prefix", placeholder = "foo")
        @NotNull String prefix
    ) {
        return new StartsWithPredicate(prefix);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input != null && input.startsWith(this.prefix);
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
        return "Starts with '" + this.prefix + "'";
    }

}
