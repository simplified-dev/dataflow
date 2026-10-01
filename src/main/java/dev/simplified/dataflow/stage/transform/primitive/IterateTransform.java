package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * {@link TransformStage} that applies a body to its own output until it stops changing the value,
 * or while a condition body holds.
 * <p>
 * Each pass first runs the optional {@code while} body against the current value; anything but
 * {@code true} ends the loop with that value. The body then yields the next value: {@code null}
 * rejects the input, as a body does everywhere else, and a value equal to the current one ends the
 * loop with it, so a body with no condition runs until it reaches a fixed point. A loop still
 * running after {@code maxIterations} passes fails the run rather than handing on a value the loop
 * did not finish, and so does a value that outgrows {@link #MAX_VALUE_SIZE} - characters of a
 * {@link CharSequence}, elements of a {@link Collection} - since a body that appends on every pass
 * would otherwise exhaust the heap well inside the pass cap.
 * <p>
 * A {@code null} input stays {@code null}.
 *
 * @param <T> the value type the body consumes and produces
 */
@StageSpec(
    id = "TRANSFORM_ITERATE",
    displayName = "Iterate",
    description = "T -> T (body: T -> T; while: T -> BOOLEAN)",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IterateTransform<T> implements TransformStage<T, T> {

    /** The most passes a stage may be configured to run. */
    public static final int MAX_ITERATIONS_CEILING = 10_000;

    /** The largest value a pass may yield: characters of a string, elements of a list. */
    public static final int MAX_VALUE_SIZE = 10_000_000;

    private final @NotNull DataType<T> type;

    private final @NotNull Chain<T, T> body;

    /**
     * Condition that must yield {@code true} for another pass to run, or {@code null} when the loop
     * runs until the body stops changing the value.
     */
    private final @Nullable Chain<T, Boolean> condition;

    /**
     * Passes the loop may run before it fails the run.
     */
    private final int maxIterations;

    /**
     * Constructs an iterate stage.
     *
     * @param type the value type the body consumes and produces
     * @param body the pass, consuming and producing {@code T}
     * @param condition the condition checked before each pass, consuming {@code T} and producing
     *                  {@code BOOLEAN}, or {@code null} to run until the body stops changing the value
     * @param maxIterations the most passes the loop runs before it fails the run, from 1 to
     *                      {@link #MAX_ITERATIONS_CEILING}
     * @return the stage
     * @param <T> the value type
     * @throws IllegalArgumentException when a body fails type-chain validation, or when
     *         {@code maxIterations} is outside 1 to {@link #MAX_ITERATIONS_CEILING}
     */
    public static <T> @NotNull IterateTransform<T> of(
        @Configurable(label = "Type", placeholder = "STRING")
        @NotNull DataType<T> type,
        @Configurable(label = "Body")
        @NotNull List<? extends Stage<?, ?>> body,
        @Configurable(name = "while", label = "While condition (optional)", optional = true)
        @Nullable List<? extends Stage<?, ?>> condition,
        @Configurable(label = "Max iterations", placeholder = "100")
        int maxIterations
    ) {
        requireValid("body", Chain.validate(type, body, type));

        if (condition != null)
            requireValid("while", Chain.validate(type, condition, DataTypes.BOOLEAN));

        if (maxIterations < 1 || maxIterations > MAX_ITERATIONS_CEILING)
            throw new IllegalArgumentException(String.format(
                "IterateTransform maxIterations '%s' is outside 1 to %s", maxIterations, MAX_ITERATIONS_CEILING
            ));

        return new IterateTransform<>(type, Chain.of(body), condition == null ? null : Chain.of(condition), maxIterations);
    }

    private static void requireValid(@NotNull String slot, @NotNull ValidationReport report) {
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid IterateTransform " + slot + ": " + report.issues());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable T input) {
        T value = input;

        for (int pass = 0; pass < this.maxIterations; pass++) {
            if (value == null) return null;

            if (this.condition != null && !Boolean.TRUE.equals(this.condition.execute(ctx, value)))
                return value;

            T next = this.body.execute(ctx, value);
            if (next == null) return null;
            requireWithinBudget(next);

            if (Objects.equals(next, value)) return value;
            value = next;
        }

        throw new IllegalStateException(String.format(
            "IterateTransform is still changing the value after maxIterations '%s' passes", this.maxIterations
        ));
    }

    private static void requireWithinBudget(@NotNull Object value) {
        long size = value instanceof CharSequence text ? text.length()
            : value instanceof Collection<?> elements ? elements.size()
            : 0;

        if (size > MAX_VALUE_SIZE)
            throw new IllegalStateException(String.format(
                "IterateTransform pass yields a value of size '%s', above %s", size, MAX_VALUE_SIZE
            ));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> inputType() {
        return this.type;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.type;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Iterate " + this.type.label() + " (" + this.body.size() + " stage"
            + (this.body.size() == 1 ? "" : "s") + (this.condition != null ? ", while" : ", until stable")
            + ", max " + this.maxIterations + ")";
    }

}
