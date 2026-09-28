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
 * {@link TransformStage} that rounds a {@link Double} to a number of decimal places under a
 * {@link RoundingMode}, through {@link BigDecimal#valueOf(double)} and
 * {@link BigDecimal#setScale(int, RoundingMode)}.
 * <p>
 * Rounding reads the value's shortest decimal form, so {@code 2.675} at scale {@code 2} under
 * {@link RoundingMode#HALF_UP HALF_UP} is {@code 2.68}, and {@code 9.97} at scale {@code 0} is
 * {@code 10.0}. A negative scale rounds to tens, hundreds and upward. A {@code NaN} or infinite
 * input, or a result too large for a {@code double}, rejects with {@code null}. Under
 * {@link RoundingMode#UNNECESSARY UNNECESSARY} a value that needs rounding throws
 * {@link ArithmeticException}.
 */
@StageSpec(
    id = "TRANSFORM_ROUND_DOUBLE",
    displayName = "Round double",
    description = "DOUBLE -> DOUBLE",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RoundDoubleTransform implements TransformStage<Double, Double> {

    /**
     * Lowest scale rounding is carried out at. Every finite {@code double} is below half of
     * {@code 10^309}, so a lower scale rounds each value exactly as this one does, without
     * building its power of ten or leaving the range {@link BigDecimal} can scale to.
     */
    private static final int MIN_EFFECTIVE_SCALE = -309;

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
     * Constructs a round-double stage.
     *
     * @param scale the number of decimal places kept; negative to round left of the decimal point
     * @param rawMode the {@link RoundingMode} constant name, carried on the wire as {@code mode}, or
     *                {@code null} for {@link RoundingMode#HALF_UP HALF_UP}
     * @return the stage
     * @throws IllegalArgumentException when {@code rawMode} names no {@link RoundingMode}
     */
    public static @NotNull RoundDoubleTransform of(
        @Configurable(label = "Scale", placeholder = "2")
        int scale,
        @Configurable(name = "mode", label = "Rounding mode (optional)", placeholder = "HALF_UP", optional = true)
        @Nullable String rawMode
    ) {
        return new RoundDoubleTransform(scale, rawMode, roundingMode(rawMode));
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
    public @Nullable Double execute(@NotNull PipelineContext ctx, @Nullable Double input) {
        if (input == null || !Double.isFinite(input)) return null;

        BigDecimal decimal = BigDecimal.valueOf(input);
        int scale = Math.max(this.scale, MIN_EFFECTIVE_SCALE);

        if (decimal.scale() > scale)
            decimal = decimal.setScale(scale, this.mode);

        double rounded = decimal.doubleValue();
        return Double.isFinite(rounded) ? rounded : null;
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
        return "Round double to scale " + this.scale + " " + this.mode;
    }

}
