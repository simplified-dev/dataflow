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
 * {@code int} operand, as {@code input OP operand}.
 * <p>
 * An overflow throws {@link ArithmeticException}, {@link ArithmeticOperator#DIVIDE DIVIDE}
 * truncates toward zero, {@link ArithmeticOperator#MODULO MODULO} is a floored remainder, and a
 * zero operand under either of those two rejects the input with {@code null}.
 */
@StageSpec(
    id = "TRANSFORM_ARITHMETIC_INT",
    displayName = "Arithmetic int",
    description = "INT -> INT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArithmeticIntTransform implements TransformStage<Integer, Integer> {

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
    private final int operand;

    /**
     * Constructs an arithmetic-int stage computing {@code input OP operand}.
     *
     * @param rawOperator the {@link ArithmeticOperator} constant name, carried on the wire as {@code operator}
     * @param operand the right-hand operand
     * @return the stage
     * @throws IllegalArgumentException when {@code rawOperator} names no {@link ArithmeticOperator}
     */
    public static @NotNull ArithmeticIntTransform of(
        @Configurable(name = "operator", label = "Operator", placeholder = "ADD")
        @NotNull String rawOperator,
        @Configurable(label = "Operand", placeholder = "1")
        int operand
    ) {
        return new ArithmeticIntTransform(rawOperator, ArithmeticOperator.of(rawOperator), operand);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable Integer input) {
        if (input == null) return null;
        return this.operator.apply(input.intValue(), this.operand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> inputType() {
        return DataTypes.INT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> outputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Arithmetic int " + this.operator + " " + this.operand;
    }

}
