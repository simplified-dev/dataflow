package dev.sbs.dataflow.stage.terminal.match;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Stream;

/**
 * {@link CollectStage} that returns {@code true} when no element's predicate body
 * evaluates to {@link Boolean#TRUE}. Short-circuits on the first match. An empty input
 * returns {@code true}, matching {@link Stream#noneMatch}.
 *
 * @param <T> element type
 */
@StageSpec(
    displayName = "NoneMatch",
    description = "List<T> -> BOOLEAN (body: T -> BOOLEAN)",
    category = StageSpec.Category.TERMINAL_MATCH
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NoneMatchCollect<T> implements CollectStage<List<T>, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain body;

    /**
     * Constructs a none-match stage.
     *
     * @param elementType element type of the list
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <T> @NotNull NoneMatchCollect<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Predicate body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(elementType, body, DataTypes.BOOLEAN);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid noneMatch body: " + report.issues());
        return new NoneMatchCollect<>(
            elementType,
            DataType.list(elementType),
            Chain.of(body)
        );
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        for (T element : input) {
            Boolean ok = this.body.execute(ctx, element);
            if (Boolean.TRUE.equals(ok)) return false;
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_NONE_MATCH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "NoneMatch " + this.elementType.label();
    }

}
