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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;

/**
 * {@link TransformStage} that rounds a {@link Float} to a number of decimal places under a
 * {@link RoundingMode}, through a {@link BigDecimal} of {@link Float#toString(float)} and
 * {@link BigDecimal#setScale(int, RoundingMode)}.
 * <p>
 * Rounding reads the value's shortest {@code float} decimal form rather than its widened
 * {@code double}, so {@code 2.45f} at scale {@code 1} under {@link RoundingMode#HALF_EVEN HALF_EVEN}
 * is {@code 2.4f}, and {@code 9.97f} at scale {@code 0} is {@code 10.0f}. A negative scale rounds
 * to tens, hundreds and upward. A {@code NaN} or infinite input, or a result too large for a
 * {@code float}, rejects with {@code null}. Under {@link RoundingMode#UNNECESSARY UNNECESSARY} a
 * value that needs rounding throws {@link ArithmeticException}.
 */
@StageSpec(
    id = "TRANSFORM_ROUND_FLOAT",
    displayName = "Round float",
    description = "FLOAT -> FLOAT",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RoundFloatTransform implements TransformStage<Float, Float> {

    /**
     * Number of decimal places kept; negative to round left of the decimal point.
     */
    private final int scale;

    /**
     * Configured rounding mode name exactly as given, carried on the wire under {@code mode}, or
     * {@code null} when none was configured.
     */
    private final @Nullable String rawMode;

    /**
     * Rounding mode resolved from {@link #rawMode}, {@link RoundingMode#HALF_UP HALF_UP} when none
     * was configured.
     */
    private final @NotNull RoundingMode mode;

    /**
     * Constructs a round-float stage.
     *
     * @param scale the number of decimal places kept; negative to round left of the decimal point
     * @param rawMode the {@link RoundingMode} constant name, carried on the wire as {@code mode}, or
     *                {@code null} for {@link RoundingMode#HALF_UP HALF_UP}
     * @return the stage
     * @throws IllegalArgumentException when {@code rawMode} names no {@link RoundingMode}
     */
    public static @NotNull RoundFloatTransform of(
        @Configurable(label = "Scale", placeholder = "2")
        int scale,
        @Configurable(name = "mode", label = "Rounding mode (optional)", placeholder = "HALF_UP", optional = true)
        @Nullable String rawMode
    ) {
        return new RoundFloatTransform(scale, rawMode, roundingMode(rawMode));
    }

    private static @NotNull RoundingMode roundingMode(@Nullable String rawMode) {
        if (rawMode == null) return RoundingMode.HALF_UP;

        try {
            return RoundingMode.valueOf(rawMode);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                "Unknown rounding mode '" + rawMode + "', expected one of " + Arrays.toString(RoundingMode.values()), ex
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Float execute(@NotNull PipelineContext ctx, @Nullable Float input) {
        if (input == null || !Float.isFinite(input)) return null;

        BigDecimal decimal = new BigDecimal(Float.toString(input));

        if (decimal.scale() > this.scale)
            decimal = decimal.setScale(this.scale, this.mode);

        float rounded = decimal.floatValue();
        return Float.isFinite(rounded) ? rounded : null;
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
        return "Round float to scale " + this.scale + " " + this.mode;
    }

}
