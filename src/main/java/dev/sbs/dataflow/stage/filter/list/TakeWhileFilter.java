package dev.sbs.dataflow.stage.filter.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.StageChainValidator;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.stage.FilterStage;
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

import java.util.ArrayList;
import java.util.List;

/**
 * {@link FilterStage} that returns the longest prefix of the input list whose elements
 * all satisfy the predicate body. Mirrors {@link java.util.stream.Stream#takeWhile}.
 *
 * @param <T> element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TakeWhileFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull ConcurrentList<Stage<?, ?>> body;

    /**
     * Constructs a take-while filter.
     *
     * @param elementType element type of the list
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <T> @NotNull TakeWhileFilter<T> of(
        @NotNull DataType<T> elementType,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = StageChainValidator.validate(elementType, body, DataTypes.BOOLEAN);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid takeWhile body: " + report.issues());
        return new TakeWhileFilter<>(
            elementType,
            DataType.list(elementType),
            Concurrent.newUnmodifiableList((List<Stage<?, ?>>) body)
        );
    }

    /**
     * Reconstructs a take-while filter from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull TakeWhileFilter<?> fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> result = new ArrayList<>();
        for (T element : input) {
            Object current = element;
            for (Stage stage : this.body) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            if (!Boolean.TRUE.equals(current)) break;
            result.add(element);
        }
        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.FILTER_TAKE_WHILE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "TakeWhile " + this.elementType.label();
    }

}
