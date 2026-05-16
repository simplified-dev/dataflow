package dev.sbs.dataflow.stage.filter.numeric;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.meta.StageSpec;
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
 * {@link FilterStage} keeping longs in the inclusive range {@code [min, max]}.
 */
@StageSpec(
    id = "FILTER_LONG_IN_RANGE",
    displayName = "Long in [min, max]",
    description = "List<LONG> -> List<LONG>",
    category = StageSpec.Category.FILTER_NUMERIC
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongInRangeFilter implements FilterStage<Long> {

    private static final @NotNull DataType<List<Long>> LIST_LONG = DataType.list(DataTypes.LONG);

    private final long min;

    private final long max;

    /**
     * Constructs a long in-range filter, inclusive on both ends.
     *
     * @param min inclusive lower bound
     * @param max inclusive upper bound
     * @return the stage
     */
    public static @NotNull LongInRangeFilter of(
        @Configurable(label = "Min (inclusive)", placeholder = "0")
        long min,
        @Configurable(label = "Max (inclusive)", placeholder = "100")
        long max
    ) {
        return new LongInRangeFilter(min, max);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<Long> execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        return input == null ? null : input.stream()
            .filter(l -> l != null && l >= this.min && l <= this.max)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> inputType() {
        return LIST_LONG;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<Long>> outputType() {
        return LIST_LONG;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Long in [" + this.min + ", " + this.max + "]";
    }

}
