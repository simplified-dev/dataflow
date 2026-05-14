package dev.sbs.dataflow.stage.terminal.match;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.StageChainValidator;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Stage;
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
 * {@link CollectStage} that returns {@code true} when no element's predicate body
 * evaluates to {@link Boolean#TRUE}. Short-circuits on the first match. An empty input
 * returns {@code true}, matching {@link java.util.stream.Stream#noneMatch}.
 *
 * @param <T> element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class NoneMatchCollect<T> implements CollectStage<List<T>, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull ConcurrentList<Stage<?, ?>> body;

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
        @NotNull DataType<T> elementType,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = StageChainValidator.validate(elementType, body, DataTypes.BOOLEAN);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid noneMatch body: " + report.issues());
        return new NoneMatchCollect<>(
            elementType,
            DataType.list(elementType),
            Concurrent.newUnmodifiableList((List<Stage<?, ?>>) body)
        );
    }

    /**
     * Reconstructs a none-match stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull NoneMatchCollect<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> elementType = cfg.getDataType("elementType");
        ConcurrentList<Stage<?, ?>> body = cfg.getSubPipeline("body");
        return of((DataType) elementType, body);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .subPipeline("body", this.body)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        for (T element : input) {
            Object current = element;
            for (Stage stage : this.body) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            if (Boolean.TRUE.equals(current)) return false;
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
