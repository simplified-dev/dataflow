package dev.sbs.dataflow.stage.transform.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that prepends a configured string in front of the input.
 */
@StageSpec(
    id = "TRANSFORM_PREFIX",
    displayName = "Prefix",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PrefixTransform implements TransformStage<String, String> {

    private final @NotNull String prefix;

    /**
     * Constructs a prefix stage.
     *
     * @param prefix the string to prepend
     * @return the stage
     */
    public static @NotNull PrefixTransform of(
        @Configurable(label = "Prefix", placeholder = ">>>")
        @NotNull String prefix
    ) {
        return new PrefixTransform(prefix);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : this.prefix + input;
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
        return "Prefix '" + this.prefix + "'";
    }

}
