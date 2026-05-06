package dev.sbs.dataflow.stage.filter.numeric;

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

/** {@link FilterStage} keeping longs strictly less than the configured threshold. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class LongLessThanFilter implements FilterStage<Long> {

    private static final @NotNull DataType<List<Long>> LIST_LONG = DataType.list(DataTypes.LONG);
    private final long threshold;

    /**
     * Constructs a long less-than filter.
     *
     * @param threshold elements must be strictly less than this
     * @return the stage
     */
    public static @NotNull LongLessThanFilter of(long threshold) {
        return new LongLessThanFilter(threshold);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Long>> inputType()  { return LIST_LONG; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<Long>> outputType() { return LIST_LONG; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                    { return StageId.FILTER_LONG_LESS_THAN; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                  { return "Long < " + this.threshold; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<Long> execute(@NotNull PipelineContext ctx, @Nullable List<Long> input) {
        return input == null ? null : input.stream().filter(l -> l != null && l < this.threshold).collect(Collectors.toUnmodifiableList());
    }

}
