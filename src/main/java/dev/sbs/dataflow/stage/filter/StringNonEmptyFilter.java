package dev.sbs.dataflow.stage.filter;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** {@link FilterStage} dropping null and empty strings. */
public final class StringNonEmptyFilter implements FilterStage<String> {

    private static final @NotNull DataType<List<String>> LIST_STRING = DataType.list(DataTypes.STRING);

    /**
     * Constructs a non-empty-string filter.
     *
     * @return the stage
     */
    public static @NotNull StringNonEmptyFilter create() {
        return new StringNonEmptyFilter();
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> inputType()  { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<String>> outputType() { return LIST_STRING; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                      { return StageId.FILTER_STRING_NON_EMPTY; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                    { return "Non-empty string"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<String> execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        return input == null ? null : input.stream().filter(s -> s != null && !s.isEmpty()).collect(Collectors.toUnmodifiableList());
    }

}
