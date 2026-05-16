package dev.sbs.dataflow.stage.terminal.average;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns the arithmetic mean of a {@code List<Long>} as a
 * {@link Double}. Returns {@code null} for empty input. Skips {@code null} elements.
 */
@StageSpec(
    id = "COLLECT_AVERAGE_LONG",
    displayName = "Average LONG",
    description = "List<LONG> -> DOUBLE",
    category = StageSpec.Category.TERMINAL_AVERAGE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AverageLongCollect implements CollectStage<List<Long>, Double> {

    private static final @NotNull DataType<List<Long>> INPUT = DataType.list(DataTypes.LONG);

    /**
     * Constructs an average-long stage.
     *
     * @return the stage
     */
    public static @NotNull AverageLongCollect of() {
        return new AverageLongCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        if (input == null) return null;
        double total = 0.0;
        int count = 0;
        for (Long element : input) {
            if (element == null) continue;
            total += element;
            count++;
        }
        return count == 0 ? null : total / count;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> inputType() {
        return INPUT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Average LONG";
    }

}
