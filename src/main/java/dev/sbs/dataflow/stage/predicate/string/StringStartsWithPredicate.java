package dev.sbs.dataflow.stage.predicate.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** {@link TransformStage} that returns {@code true} when the input starts with the configured prefix. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringStartsWithPredicate implements TransformStage<String, Boolean> {

    private final @NotNull String prefix;

    /**
     * Constructs a starts-with predicate.
     *
     * @param prefix the prefix to require
     * @return the stage
     */
    public static @NotNull StringStartsWithPredicate of(@NotNull String prefix) {
        return new StringStartsWithPredicate(prefix);
    }

    /**
     * Reconstructs a starts-with predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull StringStartsWithPredicate fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("prefix"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("prefix", this.prefix)
            .build();
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
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_STRING_STARTS_WITH;
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
