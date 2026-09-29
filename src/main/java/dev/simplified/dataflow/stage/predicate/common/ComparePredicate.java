package dev.simplified.dataflow.stage.predicate.common;

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
import dev.simplified.dataflow.stage.filter.list.WhereFilter;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link TransformStage} that derives two values of one input through a left and a right
 * sub-pipeline body and compares them with a {@link CompareOperator}, as {@code left OP right}.
 * <p>
 * The left body runs first; when it yields {@code null} the stage yields {@code null} and the
 * right body does not run, and a {@code null} from the right body yields {@code null} too, so a
 * {@link WhereFilter} over this predicate drops an element either side cannot be read from.
 * <p>
 * The values are of one configured type, and that type decides how they compare:
 * <ul>
 *   <li><b>{@code INT}, {@code LONG}, {@code FLOAT} and {@code DOUBLE}</b> - numerically, so
 *       {@code -0.0} equals {@code 0.0}; a {@code NaN} on either side yields {@code null}.</li>
 *   <li><b>{@code STRING}</b> - by {@link String#compareTo(String)}, which orders by UTF-16 code
 *       unit and is case-sensitive.</li>
 *   <li><b>{@code BOOLEAN}</b> - by equality alone, so only {@link CompareOperator#EQUALS EQUALS}
 *       and {@link CompareOperator#NOT_EQUALS NOT_EQUALS} are accepted.</li>
 * </ul>
 *
 * @param <I> input type
 * @param <V> type of the two compared values
 */
@StageSpec(
    id = "PREDICATE_COMPARE",
    displayName = "Compare",
    description = "I -> BOOLEAN (left, right: I -> V)",
    category = StageSpec.Category.PREDICATE_COMMON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ComparePredicate<I, V> implements TransformStage<I, Boolean> {

    /**
     * Value types the stage compares, in the order a refusal names them.
     */
    private static final @NotNull List<DataType<?>> VALUE_TYPES = List.of(
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.STRING, DataTypes.BOOLEAN
    );

    /**
     * Type of the input both bodies consume.
     */
    private final @NotNull DataType<I> inputType;

    /**
     * Type both bodies produce, which decides how the two values compare.
     */
    private final @NotNull DataType<V> valueType;

    /**
     * Configured operator name exactly as given, carried on the wire under {@code operator}.
     */
    private final @NotNull String rawOperator;

    /**
     * Operator resolved from {@link #rawOperator}.
     */
    private final @NotNull CompareOperator operator;

    /**
     * Body deriving the left value from the input.
     */
    private final @NotNull Chain<I, V> left;

    /**
     * Body deriving the right value from the input.
     */
    private final @NotNull Chain<I, V> right;

    /**
     * Constructs a compare predicate testing {@code left OP right} over each input.
     *
     * @param inputType the type both bodies consume
     * @param valueType the type both bodies produce; one of {@code INT}, {@code LONG},
     *                  {@code FLOAT}, {@code DOUBLE}, {@code STRING} or {@code BOOLEAN}
     * @param rawOperator the {@link CompareOperator} constant name, carried on the wire as {@code operator}
     * @param left the body deriving the left value, consuming {@code I} and producing {@code V}
     * @param right the body deriving the right value, consuming {@code I} and producing {@code V}
     * @return the stage
     * @param <I> input type
     * @param <V> type of the two compared values
     * @throws IllegalArgumentException when {@code valueType} is not supported, {@code rawOperator}
     *         names no {@link CompareOperator} or orders {@code BOOLEAN} values, or either body fails
     *         type-chain validation
     */
    public static <I, V> @NotNull ComparePredicate<I, V> of(
        @Configurable(label = "Input type", placeholder = "JSON_OBJECT")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Value type", placeholder = "INT")
        @NotNull DataType<V> valueType,
        @Configurable(name = "operator", label = "Operator", placeholder = "EQUALS")
        @NotNull String rawOperator,
        @Configurable(label = "Left value body")
        @NotNull List<? extends Stage<?, ?>> left,
        @Configurable(label = "Right value body")
        @NotNull List<? extends Stage<?, ?>> right
    ) {
        if (!VALUE_TYPES.contains(valueType)) {
            throw new IllegalArgumentException(String.format(
                "ComparePredicate supports value types %s but got '%s'", VALUE_TYPES, valueType
            ));
        }

        CompareOperator operator = CompareOperator.of(rawOperator);

        if (operator.isOrdering() && DataTypes.BOOLEAN.equals(valueType)) {
            throw new IllegalArgumentException(String.format(
                "ComparePredicate compares BOOLEAN values only with EQUALS or NOT_EQUALS but got '%s'", rawOperator
            ));
        }

        validate(inputType, valueType, "left", left);
        validate(inputType, valueType, "right", right);
        return new ComparePredicate<>(inputType, valueType, rawOperator, operator, Chain.of(left), Chain.of(right));
    }

    private static void validate(
        @NotNull DataType<?> inputType,
        @NotNull DataType<?> valueType,
        @NotNull String name,
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        ValidationReport report = Chain.validate(inputType, body, valueType);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid ComparePredicate body '" + name + "': " + report.issues());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;

        V leftValue = this.left.execute(ctx, input);
        if (leftValue == null) return null;

        V rightValue = this.right.execute(ctx, input);
        if (rightValue == null) return null;

        Integer comparison = compare(leftValue, rightValue);
        return comparison == null ? null : this.operator.test(comparison);
    }

    private static @Nullable Integer compare(@NotNull Object left, @NotNull Object right) {
        return switch (left) {
            case Integer value -> Integer.compare(value, (Integer) right);
            case Long value -> Long.compare(value, (Long) right);
            case Float value -> compare(value.doubleValue(), ((Float) right).doubleValue());
            case Double value -> compare(value.doubleValue(), ((Double) right).doubleValue());
            case String value -> value.compareTo((String) right);
            case Boolean value -> Boolean.compare(value, (Boolean) right);
            default -> throw new IllegalStateException(String.format(
                "ComparePredicate cannot compare a '%s' value", left.getClass().getName()
            ));
        };
    }

    private static @Nullable Integer compare(double left, double right) {
        if (Double.isNaN(left) || Double.isNaN(right)) return null;
        return left < right ? -1 : (left > right ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Compare " + this.valueType.label() + " " + this.operator + " over " + this.inputType.label();
    }

}
