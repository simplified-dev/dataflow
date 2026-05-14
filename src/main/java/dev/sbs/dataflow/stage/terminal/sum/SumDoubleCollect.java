package dev.sbs.dataflow.stage.terminal.sum;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that sums every element of a {@code List<Double>}. Returns
 * {@code 0.0} for an empty list and skips {@code null} elements.
 */
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SumDoubleCollect implements CollectStage<List<Double>, Double> {

    private static final @NotNull DataType<List<Double>> INPUT = DataType.list(DataTypes.DOUBLE);

    /**
     * Constructs a sum-double stage.
     *
     * @return the stage
     */
    public static @NotNull SumDoubleCollect of() {
        return new SumDoubleCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.empty();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable List<Double> input) {
        if (input == null) return null;
        double total = 0.0;
        for (Double element : input) {
            if (element != null) total += element;
        }
        return total;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Double>> inputType() {
        return INPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_SUM_DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Sum DOUBLE";
    }

}
