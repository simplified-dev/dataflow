package dev.simplified.dataflow.stage.filter.string;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.FilterStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link FilterStage} keeping every string that starts with the configured prefix.
 */
@StageSpec(
    id = "FILTER_STRING_STARTS_WITH",
    displayName = "Starts with",
    description = "List<STRING> -> List<STRING>",
    category = StageSpec.Category.FILTER_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StartsWithFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    private final @NotNull String prefix;

    /**
     * Constructs a starts-with filter.
     *
     * @param prefix the prefix to require
     * @return the stage
     */
    public static @NotNull StartsWithFilter of(
        @Configurable(label = "Prefix", placeholder = "foo")
        @NotNull String prefix
    ) {
        return new StartsWithFilter(prefix);
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
    public @NotNull DataType<List<String>> outputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Starts with '" + this.prefix + "'";
    }

}
