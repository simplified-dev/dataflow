package dev.sbs.dataflow.stage.filter.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
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
 * {@link FilterStage} keeping every string that ends with the configured suffix.
 */
@StageSpec(
    id = "FILTER_STRING_ENDS_WITH",
    displayName = "Ends with",
    description = "List<STRING> -> List<STRING>",
    category = StageSpec.Category.FILTER_STRING
)
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
    public static @NotNull StringEndsWithFilter of(
        @Configurable(label = "Suffix", placeholder = "bar")
        @NotNull String suffix
    ) {
        return new StringEndsWithFilter(suffix);
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
    public @NotNull DataType<List<String>> outputType() {
        return LIST_STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Ends with '" + this.suffix + "'";
    }

}
