package dev.sbs.dataflow.stage.transform.string;

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

/**
 * {@link TransformStage} that appends a configured string after the input.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SuffixTransform implements TransformStage<String, String> {

    private final @NotNull String suffix;

    /**
     * Constructs a suffix stage.
     *
     * @param suffix the string to append
     * @return the stage
     */
    public static @NotNull SuffixTransform of(@NotNull String suffix) {
        return new SuffixTransform(suffix);
    }

    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> inputType()  { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<String> outputType() { return DataTypes.STRING; }
    /** {@inheritDoc} */
    @Override public @NotNull StageKind kind()                { return StageKind.TRANSFORM_SUFFIX; }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("suffix", this.suffix)
            .build();
    }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()              { return "Suffix '" + this.suffix + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : input + this.suffix;
    }

}
