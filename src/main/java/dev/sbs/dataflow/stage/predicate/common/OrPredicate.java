package dev.sbs.dataflow.stage.predicate.common;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
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
 * {@link TransformStage} that returns {@code true} as soon as any named sub-pipeline body
 * yields {@link Boolean#TRUE}. Short-circuits on the first true. An empty body map yields
 * {@code false} (vacuous disjunction).
 *
 * @param <T> input element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class OrPredicate<T> implements TransformStage<T, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull NamedChains bodies;

    /**
     * Constructs an or-predicate from a map of named predicate bodies.
     *
     * @param elementType the element type each body consumes
     * @param bodies named predicate bodies; each body's first stage must consume {@code T}
     *               and its last stage must produce {@code BOOLEAN}
     * @return the stage
     * @param <T> input element type
     * @throws IllegalArgumentException when any body fails type-chain validation
     */
    public static <T> @NotNull OrPredicate<T> of(
        @NotNull DataType<T> elementType,
        @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> bodies
    ) {
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : bodies.entrySet()) {
            ValidationReport report = Chain.validate(elementType, entry.getValue(), DataTypes.BOOLEAN);
            if (!report.isValid())
                throw new IllegalArgumentException(
                    "Invalid OrPredicate body '" + entry.getKey() + "': " + report.issues()
                );
        }
        return new OrPredicate<>(elementType, NamedChains.of(bodies));
    }

    /**
     * Reconstructs an or-predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull OrPredicate<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> elementType = cfg.getDataType("elementType");
        NamedChains bodies = cfg.getSubPipelines("bodies");
        Map<String, List<Stage<?, ?>>> raw = new LinkedHashMap<>();
        for (Map.Entry<String, Chain> entry : bodies.chains().entrySet())
            raw.put(entry.getKey(), entry.getValue().stages());
        return of((DataType) elementType, raw);
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
    @Override
    public @NotNull Boolean execute(@NotNull PipelineContext ctx, @Nullable T input) {
        for (Map.Entry<String, Chain> entry : this.bodies.chains().entrySet()) {
            Boolean ok = entry.getValue().execute(ctx, input);
            if (Boolean.TRUE.equals(ok)) return true;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> inputType() {
        return this.elementType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_OR;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Or (" + this.bodies.size() + " predicate" + (this.bodies.size() == 1 ? "" : "s") + ")";
    }

}
