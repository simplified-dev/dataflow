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
 * {@link CollectStage} that sums every element of a {@code List<Integer>}. Returns
 * {@code 0} for an empty list and skips {@code null} elements.
 */
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SumIntCollect implements CollectStage<List<Integer>, Integer> {

    private static final @NotNull DataType<List<Integer>> INPUT = DataType.list(DataTypes.INT);

    /**
     * Constructs a sum-int stage.
     *
     * @return the stage
     */
    public static @NotNull SumIntCollect of() {
        return new SumIntCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.empty();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        if (input == null) return null;
        int total = 0;
        for (Integer element : input) {
            if (element != null) total += element;
        }
        return total;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> inputType() {
        return INPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_SUM_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> outputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Sum INT";
    }

}
