package dev.sbs.dataflow.stage.terminal.average;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns the arithmetic mean of a {@code List<Integer>} as a
 * {@link Double}. Returns {@code null} for empty input. Skips {@code null} elements.
 */
@StageSpec(
    displayName = "Average INT",
    description = "List<INT> -> DOUBLE",
    category = StageSpec.Category.TERMINAL_AVERAGE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AverageIntCollect implements CollectStage<List<Integer>, Double> {

    private static final @NotNull DataType<List<Integer>> INPUT = DataType.list(DataTypes.INT);

    /**
     * Constructs an average-int stage.
     *
     * @return the stage
     */
    public static @NotNull AverageIntCollect of() {
        return new AverageIntCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        if (input == null) return null;
        long total = 0L;
        int count = 0;
        for (Integer element : input) {
            if (element == null) continue;
            total += element;
            count++;
        }
        return count == 0 ? null : (double) total / count;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> inputType() {
        return INPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_AVERAGE_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Average INT";
    }

}
