package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that joins a {@code List<String>} into a single string with a
 * configurable separator.
 */
@StageSpec(
    id = "COLLECT_JOIN",
    displayName = "Join",
    description = "List<STRING> -> STRING",
    category = StageSpec.Category.TERMINAL_COLLECT
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JoinCollect implements CollectStage<List<String>, String> {

    private static final @NotNull DataType<List<String>> INPUT = DataType.list(DataTypes.STRING);

    private final @NotNull String separator;

    /**
     * Constructs a join stage.
     *
     * @param separator the separator inserted between each element
     * @return the stage
     */
    public static @NotNull JoinCollect of(
        @Configurable(label = "Separator", placeholder = ", ")
        @NotNull String separator
    ) {
        return new JoinCollect(separator);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable List<String> input) {
        if (input == null) return null;
        return String.join(this.separator, input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> inputType() {
        return INPUT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Join with '" + this.separator + "'";
    }

}
