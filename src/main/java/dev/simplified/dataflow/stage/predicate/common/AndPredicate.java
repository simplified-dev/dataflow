package dev.simplified.dataflow.stage.predicate.common;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * {@link TransformStage} that returns {@code true} iff every named sub-pipeline body yields
 * {@link Boolean#TRUE} for the input. Short-circuits on the first body that yields anything
 * else. An empty body map yields {@code true} (vacuous conjunction).
 *
 * @param <T> input element type
 */
@StageSpec(
    id = "PREDICATE_AND",
    displayName = "And",
    description = "T -> BOOLEAN (AND over N predicate bodies)",
    category = StageSpec.Category.PREDICATE_COMMON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AndPredicate<T> implements TransformStage<T, Boolean> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull NamedChains<T> bodies;

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
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Predicate bodies")
        @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> bodies
    ) {
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : bodies.entrySet()) {
            ValidationReport report = Chain.validate(elementType, entry.getValue(), DataTypes.BOOLEAN);
            if (!report.isValid())
                throw new IllegalArgumentException(
                    "Invalid AndPredicate body '" + entry.getKey() + "': " + report.issues()
                );
        }
        return new AndPredicate<>(elementType, NamedChains.of(bodies));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Boolean execute(@NotNull PipelineContext ctx, @Nullable T input) {
        for (Map.Entry<String, Chain<T, ?>> entry : this.bodies.chains().entrySet()) {
            Object ok = entry.getValue().execute(ctx, input);
            if (!Boolean.TRUE.equals(ok)) return false;
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
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "And (" + this.bodies.size() + " predicate" + (this.bodies.size() == 1 ? "" : "s") + ")";
    }

}
