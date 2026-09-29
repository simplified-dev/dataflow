package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that applies an {@link ArithmeticOperator} to its input and a configured
 * {@code double} operand, as {@code input OP operand}.
 * <p>
 * {@link ArithmeticOperator#MODULO MODULO} is a floored remainder. A {@code NaN} or infinite
 * result, or a zero operand under {@link ArithmeticOperator#DIVIDE DIVIDE} or
 * {@link ArithmeticOperator#MODULO MODULO}, rejects the input with {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_ARITHMETIC_DOUBLE",
    displayName = "Arithmetic double",
    description = "DOUBLE -> DOUBLE",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArithmeticDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Configured operator name exactly as given, carried on the wire under {@code operator}.
     */
    private final @NotNull String rawOperator;

    /**
     * Operator resolved from {@link #rawOperator}.
     */
    private final @NotNull ArithmeticOperator operator;

    /**
     * Right-hand operand applied to every input.
     */
    private final double operand;

    /**
     * Constructs an arithmetic-double stage computing {@code input OP operand}.
     *
     * @param rawOperator the {@link ArithmeticOperator} constant name, carried on the wire as {@code operator}
     * @param operand the right-hand operand
     * @return the stage
     * @throws IllegalArgumentException when {@code rawOperator} names no {@link ArithmeticOperator}, or
     *         {@code operand} is {@code NaN} or infinite
     */
    public static @NotNull ArithmeticDoubleTransform of(
        @Configurable(name = "operator", label = "Operator", placeholder = "ADD")
        @NotNull String rawOperator,
        @Configurable(label = "Operand", placeholder = "1.5")
        double operand
    ) {
        ArithmeticOperator operator = ArithmeticOperator.of(rawOperator);

        if (!Double.isFinite(operand))
            throw new IllegalArgumentException("ArithmeticDoubleTransform operand '" + operand + "' is not finite");

        return new ArithmeticDoubleTransform(rawOperator, operator, operand);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        if (input == null) return null;
        return this.operator.apply(input.doubleValue(), this.operand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> inputType() {
        return DataTypes.DOUBLE;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Double> outputType() {
        return DataTypes.DOUBLE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Arithmetic double " + this.operator + " " + this.operand;
    }

}
