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
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;

/**
 * {@link TransformStage} that replaces every present input with one configured constant - the
 * body-side counterpart of {@link LiteralSource}, which is legal at stage 0 only.
 * <p>
 * A {@code null} input stays {@code null}, so inside a body the constant marks present elements
 * only. The constant is parsed from its configured string once, when the stage is built, under
 * the same types {@link LiteralSource} admits: {@code STRING}, {@code RAW_HTML}, {@code RAW_XML}
 * and {@code RAW_JSON} verbatim, {@code INT}, {@code LONG}, {@code FLOAT} and {@code DOUBLE}
 * after trimming, and {@code BOOLEAN} from {@code true} or {@code false} in any case.
 *
 * @param <I> input type
 * @param <T> constant type
 */
@StageSpec(
    id = "TRANSFORM_CONSTANT",
    displayName = "Constant",
    description = "I -> T",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConstantTransform<I, T> implements TransformStage<I, T> {

    private static final @NotNull Set<DataType<?>> STRING_LIKE = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    private static final @NotNull List<DataType<?>> SUPPORTED_TYPES = List.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON,
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.BOOLEAN
    );

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<T> outputType;

    /**
     * Configured string exactly as given, carried on the wire under {@code value}.
     */
    private final @NotNull String value;

    /**
     * Configured string parsed under {@link #outputType}, emitted for every present input.
     */
    private final @NotNull T constant;

    /**
     * Constructs a constant stage that emits {@code value} parsed under {@code outputType}.
     *
     * @param inputType the type of the values the constant replaces
     * @param outputType the type of the constant
     * @param value the constant's serialized form
     * @return the stage
     * @param <I> input type
     * @param <T> constant type
     * @throws IllegalArgumentException when {@code outputType} is not a type the stage admits, or
     *         {@code value} does not parse as {@code outputType}
     */
    @SuppressWarnings("unchecked")
    public static <I, T> @NotNull ConstantTransform<I, T> of(
        @Configurable(label = "Input type", placeholder = "STRING")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Output type", placeholder = "STRING")
        @NotNull DataType<T> outputType,
        @Configurable(label = "Value", placeholder = "constant")
        @NotNull String value
    ) {
        T constant = (T) parse("ConstantTransform", "value", outputType, value);
        return new ConstantTransform<>(inputType, outputType, value, constant);
    }

    /**
     * Parses a configured string under {@code type}, as a constant of this stage is parsed.
     *
     * @param stage the name of the stage being built, for the exception message
     * @param slot the name of the configuration slot holding {@code raw}, for the exception message
     * @param type the type to parse under
     * @param raw the configured string
     * @return the parsed value
     * @throws IllegalArgumentException when {@code type} is not a type the stage admits, or
     *         {@code raw} does not parse as {@code type}
     */
    static @NotNull Object parse(@NotNull String stage, @NotNull String slot, @NotNull DataType<?> type, @NotNull String raw) {
        if (STRING_LIKE.contains(type)) return raw;

        if (!SUPPORTED_TYPES.contains(type))
            throw new IllegalArgumentException(
                stage + " cannot parse " + slot + " as " + type.label() + "; it parses " + labels()
            );

        String trimmed = raw.trim();

        try {
            if (type.equals(DataTypes.INT)) return Integer.valueOf(trimmed);
            if (type.equals(DataTypes.LONG)) return Long.valueOf(trimmed);
            if (type.equals(DataTypes.FLOAT)) return Float.valueOf(trimmed);
            if (type.equals(DataTypes.DOUBLE)) return Double.valueOf(trimmed);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                stage + " " + slot + " '" + raw + "' does not parse as " + type.label(), ex
            );
        }

        if (trimmed.equalsIgnoreCase("true")) return Boolean.TRUE;
        if (trimmed.equalsIgnoreCase("false")) return Boolean.FALSE;

        throw new IllegalArgumentException(
            stage + " " + slot + " '" + raw + "' does not parse as " + type.label()
        );
    }

    private static @NotNull List<String> labels() {
        return SUPPORTED_TYPES.stream().map(DataType::label).toList();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        return this.constant;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Constant " + this.outputType.label() + " '" + this.value + "'";
    }

}
