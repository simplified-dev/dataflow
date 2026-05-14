package dev.sbs.dataflow.stage.predicate.common;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.StageChainValidator;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TransformStage} that returns {@code true} iff every named sub-pipeline body yields
 * {@link Boolean#TRUE} for the input. Short-circuits on the first body that yields anything
 * else. An empty body map yields {@code true} (vacuous conjunction).
 *
 * @param <T> input element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AndPredicate<T> implements TransformStage<T, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull Map<String, ConcurrentList<Stage<?, ?>>> bodies;

    /**
     * Constructs an and-predicate from a map of named predicate bodies.
     *
     * @param elementType the element type each body consumes
     * @param bodies named predicate bodies; each body's first stage must consume {@code T}
     *               and its last stage must produce {@code BOOLEAN}
     * @return the stage
     * @param <T> input element type
     * @throws IllegalArgumentException when any body fails type-chain validation
     */
    public static <T> @NotNull AndPredicate<T> of(
        @NotNull DataType<T> elementType,
        @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> bodies
    ) {
        Map<String, ConcurrentList<Stage<?, ?>>> frozen = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : bodies.entrySet()) {
            ValidationReport report = StageChainValidator.validate(elementType, entry.getValue(), DataTypes.BOOLEAN);
            if (!report.isValid())
                throw new IllegalArgumentException(
                    "Invalid AndPredicate body '" + entry.getKey() + "': " + report.issues()
                );
            frozen.put(entry.getKey(), Concurrent.newUnmodifiableList((List<Stage<?, ?>>) entry.getValue()));
        }
        return new AndPredicate<>(elementType, Map.copyOf(frozen));
    }

    /**
     * Reconstructs an and-predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull AndPredicate<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> elementType = cfg.getDataType("elementType");
        Map<String, ConcurrentList<Stage<?, ?>>> bodies = cfg.getSubPipelines("bodies");
        return of((DataType) elementType, bodies);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .subPipelines("bodies", this.bodies)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public @NotNull Boolean execute(@NotNull PipelineContext ctx, @Nullable T input) {
        for (Map.Entry<String, ConcurrentList<Stage<?, ?>>> entry : this.bodies.entrySet()) {
            Object current = input;
            for (Stage stage : entry.getValue()) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            if (!Boolean.TRUE.equals(current)) return false;
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> inputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_AND;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "And (" + this.bodies.size() + " predicate" + (this.bodies.size() == 1 ? "" : "s") + ")";
    }

}
