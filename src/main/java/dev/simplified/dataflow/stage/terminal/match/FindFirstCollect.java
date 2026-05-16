package dev.simplified.dataflow.stage.terminal.match;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.CollectStage;
import dev.simplified.dataflow.stage.Stage;
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
 * {@link CollectStage} that returns the first element for which the predicate body
 * evaluates to {@link Boolean#TRUE}. Returns {@code null} when no element matches or the
 * input is empty / null.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "COLLECT_FIND_FIRST",
    displayName = "FindFirst (predicate)",
    description = "List<T> -> T (body: T -> BOOLEAN)",
    category = StageSpec.Category.TERMINAL_MATCH
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FindFirstCollect<T> implements CollectStage<List<T>, T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain body;

    /**
     * Constructs a find-first stage.
     *
     * @param elementType element type of the list
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <T> @NotNull FindFirstCollect<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Predicate body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(elementType, body, DataTypes.BOOLEAN);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid findFirst body: " + report.issues());
        return new FindFirstCollect<>(
            elementType,
            DataType.list(elementType),
            Chain.of(body)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        for (T element : input) {
            Boolean ok = this.body.execute(ctx, element);
            if (Boolean.TRUE.equals(ok)) return element;
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "FindFirst " + this.elementType.label();
    }

}
