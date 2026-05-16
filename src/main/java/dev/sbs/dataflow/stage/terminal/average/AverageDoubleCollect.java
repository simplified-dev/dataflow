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
 * {@link CollectStage} that returns the arithmetic mean of a {@code List<Double>}. Returns
 * {@code null} for empty input. Skips {@code null} elements.
 */
@StageSpec(
    displayName = "Average DOUBLE",
    description = "List<DOUBLE> -> DOUBLE",
    category = StageSpec.Category.TERMINAL_AVERAGE
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AverageDoubleCollect implements CollectStage<List<Double>, Double> {

    private static final @NotNull DataType<List<Double>> INPUT = DataType.list(DataTypes.DOUBLE);

    /**
     * Constructs an average-double stage.
     *
     * @return the stage
     */
    public static @NotNull AverageDoubleCollect of() {
        return new AverageDoubleCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable List<Double> input) {
        if (input == null) return null;
        double total = 0.0;
        int count = 0;
        for (Double element : input) {
            if (element == null) continue;
            total += element;
            count++;
        }
        return count == 0 ? null : total / count;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Double>> inputType() {
        return INPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_AVERAGE_DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Average DOUBLE";
    }

}
