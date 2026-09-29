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

import java.util.List;

/**
 * {@link TransformStage} that derives two {@code double} operands from its input through a left and
 * a right sub-pipeline body and applies an {@link ArithmeticOperator} to them, as
 * {@code left OP right}.
 * <p>
 * The left body runs first; when it yields {@code null} the input is rejected with {@code null}
 * and the right body does not run, and a {@code null} from the right body rejects it too.
 * {@link ArithmeticOperator#MODULO MODULO} is a floored remainder. A {@code NaN} or infinite
 * result, or a zero right operand under {@link ArithmeticOperator#DIVIDE DIVIDE} or
 * {@link ArithmeticOperator#MODULO MODULO}, rejects the input with {@code null}.
 *
 * @param <I> input type
 */
@StageSpec(
    id = "TRANSFORM_BINARY_ARITHMETIC_DOUBLE",
    displayName = "Binary arithmetic double",
    description = "I -> DOUBLE (left, right: I -> DOUBLE)",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class BinaryArithmeticDoubleTransform<I> implements TransformStage<I, Double> {

    /**
     * Type of the input both bodies consume.
     */
    private final @NotNull DataType<I> inputType;

    /**
     * Configured operator name exactly as given, carried on the wire under {@code operator}.
     */
    private final @NotNull String rawOperator;

    /**
     * Operator resolved from {@link #rawOperator}.
     */
    private final @NotNull ArithmeticOperator operator;

    /**
     * Body deriving the left operand from the input.
     */
    private final @NotNull Chain<I, Double> left;

    /**
     * Body deriving the right operand from the input.
     */
    private final @NotNull Chain<I, Double> right;

    /**
     * Constructs a binary arithmetic-double stage computing {@code left OP right} over each input.
     *
     * @param inputType the type both bodies consume
     * @param rawOperator the {@link ArithmeticOperator} constant name, carried on the wire as {@code operator}
     * @param left the body deriving the left operand, consuming {@code I} and producing {@code DOUBLE}
     * @param right the body deriving the right operand, consuming {@code I} and producing {@code DOUBLE}
     * @return the stage
     * @param <I> input type
     * @throws IllegalArgumentException when {@code rawOperator} names no {@link ArithmeticOperator}, or
     *         either body fails type-chain validation
     */
    public static <I> @NotNull BinaryArithmeticDoubleTransform<I> of(
        @Configurable(label = "Input type", placeholder = "JSON_OBJECT")
        @NotNull DataType<I> inputType,
        @Configurable(name = "operator", label = "Operator", placeholder = "ADD")
        @NotNull String rawOperator,
        @Configurable(label = "Left operand body")
        @NotNull List<? extends Stage<?, ?>> left,
        @Configurable(label = "Right operand body")
        @NotNull List<? extends Stage<?, ?>> right
    ) {
        ArithmeticOperator operator = ArithmeticOperator.of(rawOperator);
        validate(inputType, "left", left);
        validate(inputType, "right", right);
        return new BinaryArithmeticDoubleTransform<>(inputType, rawOperator, operator, Chain.of(left), Chain.of(right));
    }

    private static void validate(@NotNull DataType<?> inputType, @NotNull String name, @NotNull List<? extends Stage<?, ?>> body) {
        ValidationReport report = Chain.validate(inputType, body, DataTypes.DOUBLE);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid BinaryArithmeticDoubleTransform body '" + name + "': " + report.issues());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;

        Double leftValue = this.left.execute(ctx, input);
        if (leftValue == null) return null;

        Double rightValue = this.right.execute(ctx, input);
        if (rightValue == null) return null;

        return this.operator.apply(leftValue.doubleValue(), rightValue.doubleValue());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Binary arithmetic double " + this.operator + " over " + this.inputType.label();
    }

}
