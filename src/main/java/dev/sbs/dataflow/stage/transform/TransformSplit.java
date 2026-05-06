package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link TransformStage} that splits a {@link String} on a regex into a list of substrings,
 * equivalent to {@link String#split(String)}.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class TransformSplit implements TransformStage<String, List<String>> {

    private static final @NotNull DataType<List<String>> OUTPUT = DataType.list(DataTypes.STRING);

    private final @NotNull String regex;

    /**
     * Constructs a split stage.
     *
     * @param regex the regex used as a delimiter
     * @return the stage
     */
    public static @NotNull TransformSplit of(@NotNull String regex) {
        return new TransformSplit(regex);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.TRANSFORM_SPLIT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Split on '" + this.regex + "'";
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<String> execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return List.of(input.split(this.regex));
    }

}
