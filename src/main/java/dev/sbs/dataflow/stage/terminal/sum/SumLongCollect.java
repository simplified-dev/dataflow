package dev.sbs.dataflow.stage.terminal.sum;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that sums every element of a {@code List<Long>}. Returns
 * {@code 0L} for an empty list and skips {@code null} elements.
 */
@StageSpec(
    id = "COLLECT_SUM_LONG",
    displayName = "Sum LONG",
    description = "List<LONG> -> LONG",
    category = StageSpec.Category.TERMINAL_SUM
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SumLongCollect implements CollectStage<List<Long>, Long> {

    private static final @NotNull DataType<List<Long>> INPUT = DataType.list(DataTypes.LONG);

    /**
     * Constructs a sum-long stage.
     *
     * @return the stage
     */
    public static @NotNull SumLongCollect of() {
        return new SumLongCollect();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Long execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        if (input == null) return null;
        long total = 0L;
        for (Long element : input) {
            if (element != null) total += element;
        }
        return total;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> inputType() {
        return INPUT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Long> outputType() {
        return DataTypes.LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Sum LONG";
    }

}
