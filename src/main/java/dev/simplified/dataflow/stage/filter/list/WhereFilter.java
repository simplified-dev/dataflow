package dev.simplified.dataflow.stage.filter.list;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.FilterStage;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * {@link FilterStage} that keeps the elements for which a predicate body yields {@code true},
 * in their input order. Mirrors {@link Stream#filter(Predicate)}.
 * <p>
 * The body runs once per element. An element whose body yields {@code false} or {@code null} is
 * dropped, so a predicate that cannot decide an element leaves it out; a {@code null} element is
 * dropped without running the body.
 *
 * @param <T> element type
 */
@StageSpec(
    id = "FILTER_WHERE",
    displayName = "Where",
    description = "List<T> -> List<T> (body: T -> BOOLEAN)",
    category = StageSpec.Category.FILTER_LIST
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class WhereFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    private final @NotNull Chain<T, Boolean> body;

    /**
     * Constructs a where filter.
     *
     * @param elementType element type of the list
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> element type
     * @throws IllegalArgumentException when {@code body} fails type-chain validation
     */
    public static <T> @NotNull WhereFilter<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType,
        @Configurable(label = "Predicate body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(elementType, body, DataTypes.BOOLEAN);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid WhereFilter body: " + report.issues());

        return new WhereFilter<>(elementType, DataType.list(elementType), Chain.of(body));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> result = new ArrayList<>();

        for (T element : input) {
            if (Boolean.TRUE.equals(this.body.execute(ctx, element)))
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
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Where " + this.elementType.label();
    }

}
