package dev.sbs.dataflow.stage.terminal.match;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link CollectStage} that returns {@code true} when every element's predicate body
 * evaluates to {@link Boolean#TRUE}. Short-circuits on the first non-match. An empty
 * input returns {@code true}, matching {@link java.util.stream.Stream#allMatch}.
 *
 * @param <T> element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AllMatchCollect<T> implements CollectStage<List<T>, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain body;

    /**
     * Constructs an all-match stage.
     *
     * @param elementType element type of the list
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <T> @NotNull AllMatchCollect<T> of(
        @NotNull DataType<T> elementType,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(elementType, body, DataTypes.BOOLEAN);
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid allMatch body: " + report.issues());
        return new AllMatchCollect<>(
            elementType,
            DataType.list(elementType),
            Chain.of(body)
        );
    }

    /**
     * Reconstructs an all-match stage from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull AllMatchCollect<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> elementType = cfg.getDataType("elementType");
        Chain body = cfg.getSubPipeline("body");
        return of((DataType) elementType, body.stages());
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
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        for (T element : input) {
            Boolean ok = this.body.execute(ctx, element);
            if (!Boolean.TRUE.equals(ok)) return false;
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
        return StageKind.COLLECT_ALL_MATCH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "AllMatch " + this.elementType.label();
    }

}
