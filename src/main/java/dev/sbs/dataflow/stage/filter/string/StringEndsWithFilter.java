package dev.sbs.dataflow.stage.filter.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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
 * {@link FilterStage} keeping every string that ends with the configured suffix.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringEndsWithFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    private final @NotNull String suffix;

    /**
     * Constructs an ends-with filter.
     *
     * @param suffix the suffix to require
     * @return the stage
     */
    public static @NotNull StringEndsWithFilter of(@NotNull String suffix) {
        return new StringEndsWithFilter(suffix);
    }

    /**
     * Reconstructs the filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull StringEndsWithFilter fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("suffix"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("suffix", this.suffix)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream()
            .filter(s -> s != null && s.endsWith(this.suffix))
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> inputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_STRING_ENDS_WITH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> outputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Ends with '" + this.suffix + "'";
    }

}
