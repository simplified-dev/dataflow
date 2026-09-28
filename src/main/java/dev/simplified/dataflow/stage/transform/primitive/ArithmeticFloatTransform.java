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
 * operand in {@code float} arithmetic, as {@code input OP operand}.
 * <p>
 * The operand is configured as a {@code double} and narrowed to {@code float} once, when the
 * stage is built. {@link ArithmeticOperator#MODULO MODULO} is a floored remainder. A {@code NaN}
 * or infinite result, or a zero operand under {@link ArithmeticOperator#DIVIDE DIVIDE} or
 * {@link ArithmeticOperator#MODULO MODULO}, rejects the input with {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_ARITHMETIC_FLOAT",
    displayName = "Arithmetic float",
    description = "FLOAT -> FLOAT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArithmeticFloatTransform implements TransformStage<Float, Float> {

    /**
     * Configured operator name exactly as given, carried on the wire under {@code operator}.
     */
    private final @NotNull String rawOperator;

    /**
     * Operator resolved from {@link #rawOperator}.
     */
    private final @NotNull ArithmeticOperator operator;

    /**
     * Configured operand exactly as given, carried on the wire under {@code operand}.
     */
    private final double rawOperand;

    /**
     * Right-hand operand applied to every input, {@link #rawOperand} narrowed to {@code float}.
     */
    private final float operand;

    /**
     * Constructs an arithmetic-float stage computing {@code input OP operand}.
     *
     * @param rawOperator the {@link ArithmeticOperator} constant name, carried on the wire as {@code operator}
     * @param rawOperand the right-hand operand, narrowed to {@code float} and carried on the wire as {@code operand}
     * @return the stage
     * @throws IllegalArgumentException when {@code rawOperator} names no {@link ArithmeticOperator}, or
     *         {@code rawOperand} is {@code NaN}, infinite or outside the {@code float} range
     */
    public static @NotNull ArithmeticFloatTransform of(
        @Configurable(name = "operator", label = "Operator", placeholder = "ADD")
        @NotNull String rawOperator,
        @Configurable(name = "operand", label = "Operand", placeholder = "1.5")
        double rawOperand
    ) {
        ArithmeticOperator operator = ArithmeticOperator.of(rawOperator);
        float operand = (float) rawOperand;

        if (!Float.isFinite(operand))
            throw new IllegalArgumentException("ArithmeticFloatTransform operand must be a finite float but was " + rawOperand);

        return new ArithmeticFloatTransform(rawOperator, operator, rawOperand, operand);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        if (input == null) return null;
        return this.operator.apply(input.floatValue(), this.operand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Float> inputType() {
        return DataTypes.FLOAT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Float> outputType() {
        return DataTypes.FLOAT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Arithmetic float " + this.operator + " " + this.operand;
    }

}
