package dev.sbs.dataflow.stage.filter.numeric;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link FilterStage} keeping ints strictly greater than the configured threshold.
 */
@StageSpec(
    displayName = "Int >",
    description = "List<INT> -> List<INT>",
    category = StageSpec.Category.FILTER_NUMERIC
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IntGreaterThanFilter implements FilterStage<Integer> {

    private static final @NotNull DataType<List<Integer>> LIST_INT = DataType.list(DataTypes.INT);

    private final int threshold;

    /**
     * Constructs an int greater-than filter.
     *
     * @param threshold inclusive lower exclusive bound; elements must be strictly greater
     * @return the stage
     */
    public static @NotNull IntGreaterThanFilter of(
        @Configurable(label = "Threshold", placeholder = "0")
        int threshold
    ) {
        return new IntGreaterThanFilter(threshold);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Integer> execute(@NotNull PipelineContext ctx, @Nullable List<Integer> input) {
        return input == null ? null : input.stream()
            .filter(i -> i != null && i > this.threshold)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> inputType() {
        return LIST_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_INT_GREATER_THAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Integer>> outputType() {
        return LIST_INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Int > " + this.threshold;
    }

}
