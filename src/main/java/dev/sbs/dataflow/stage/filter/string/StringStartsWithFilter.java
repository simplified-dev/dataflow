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

/** {@link FilterStage} keeping every string that starts with the configured prefix. */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringStartsWithFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    private final @NotNull String prefix;

    /**
     * Constructs a starts-with filter.
     *
     * @param prefix the prefix to require
     * @return the stage
     */
    public static @NotNull StringStartsWithFilter of(@NotNull String prefix) {
        return new StringStartsWithFilter(prefix);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("prefix", this.prefix)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream()
            .filter(s -> s != null && s.startsWith(this.prefix))
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
        return StageKind.FILTER_STRING_STARTS_WITH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> outputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Starts with '" + this.prefix + "'";
    }

}
