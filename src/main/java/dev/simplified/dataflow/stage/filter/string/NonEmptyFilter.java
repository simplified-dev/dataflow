package dev.simplified.dataflow.stage.filter.string;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.FilterStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link FilterStage} dropping null and empty strings.
 */
@StageSpec(
    id = "FILTER_STRING_NON_EMPTY",
    displayName = "Non-empty",
    description = "List<STRING> -> List<STRING>",
    category = StageSpec.Category.FILTER_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NonEmptyFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    /**
     * Constructs a non-empty-string filter.
     *
     * @return the stage
     */
    public static @NotNull NonEmptyFilter of() {
        return new NonEmptyFilter();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream()
            .filter(s -> s != null && !s.isEmpty())
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
        return "Non-empty string";
    }

}
