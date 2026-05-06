package dev.sbs.dataflow.stage.filter;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** {@link FilterStage} keeping ints strictly less than the configured threshold. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class IntLessThanFilter implements FilterStage<Integer> {

    private static final @NotNull DataType<List<Integer>> LIST_INT = DataType.list(DataTypes.INT);
    private final int threshold;

    /**
     * Constructs an int less-than filter.
     *
     * @param threshold strict upper bound; elements must be strictly less than this
     * @return the stage
     */
    public static @NotNull IntLessThanFilter of(int threshold) {
        return new IntLessThanFilter(threshold);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Integer>> inputType()  { return LIST_INT; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Integer>> outputType() { return LIST_INT; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                       { return StageId.FILTER_INT_LESS_THAN; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                     { return "Int < " + this.threshold; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Integer> execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        return input == null ? null : input.stream().filter(i -> i != null && i < this.threshold).collect(Collectors.toUnmodifiableList());
    }

}
