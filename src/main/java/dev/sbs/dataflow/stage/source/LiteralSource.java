package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link SourceStage} that emits a single literal value parsed from its configuration.
 * Equivalent to {@link Stream#of(Object)}.
 * <p>
 * Supported {@code outputType}s parse from the configured string verbatim
 * ({@code STRING}, {@code RAW_HTML}, {@code RAW_XML}, {@code RAW_JSON}) or via the matching
 * {@code parseXxx} ({@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE},
 * {@code BOOLEAN}). Structured types ({@code DOM_NODE}, {@code JSON_*}) are rejected at
 * build time and should be wired via {@code LiteralSource(RAW_*) -> ParseXxxTransform}.
 *
 * @param <T> the value type
 */
@StageSpec(
    displayName = "Literal",
    description = "() -> T",
    category = StageSpec.Category.SOURCE
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LiteralSource<T> implements SourceStage<T> {

    private static final @NotNull Set<DataType<?>> STRING_LIKE = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    private final @NotNull DataType<T> outputType;

    private final @NotNull String rawValue;

    private final @NotNull T value;

    /**
     * Constructs an {@code LiteralSource} that emits {@code value} parsed under {@code outputType}.
     *
     * @param outputType the type to emit
     * @param value the value's serialized form
     * @return the stage
     * @param <T> the value type
     * @throws IllegalArgumentException when {@code outputType} is unsupported or {@code value} cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public static <T> @NotNull LiteralSource<T> of(
        @Configurable(label = "Output type", placeholder = "STRING")
        @NotNull DataType<T> outputType,
        @Configurable(label = "Value", placeholder = "literal")
        @NotNull String value
    ) {
        T parsed = (T) parse(outputType, value);
        return new LiteralSource<>(outputType, value, parsed);
    }

    /**
     * Convenience factory for an HTML body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_HTML, body)}.
     *
     * @param body the HTML body
     * @return the stage
     */
    public static @NotNull LiteralSource<String> rawHtml(@NotNull String body) {
        return of(DataTypes.RAW_HTML, body);
    }

    /**
     * Convenience factory for an XML body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_XML, body)}.
     *
     * @param body the XML body
     * @return the stage
     */
    public static @NotNull LiteralSource<String> rawXml(@NotNull String body) {
        return of(DataTypes.RAW_XML, body);
    }

    /**
     * Convenience factory for a JSON body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_JSON, body)}.
     *
     * @param body the JSON body
     * @return the stage
     */
    public static @NotNull LiteralSource<String> rawJson(@NotNull String body) {
        return of(DataTypes.RAW_JSON, body);
    }

    /**
     * Convenience factory for a plain {@link String} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.STRING, value)}.
     *
     * @param value the string value
     * @return the stage
     */
    public static @NotNull LiteralSource<String> text(@NotNull String value) {
        return of(DataTypes.STRING, value);
    }

    /**
     * Convenience factory for an {@code INT} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.INT, Integer.toString(value))}.
     *
     * @param value the int value
     * @return the stage
     */
    public static @NotNull LiteralSource<Integer> integerVal(int value) {
        return of(DataTypes.INT, Integer.toString(value));
    }

    /**
     * Convenience factory for a {@code LONG} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.LONG, Long.toString(value))}.
     *
     * @param value the long value
     * @return the stage
     */
    public static @NotNull LiteralSource<Long> longVal(long value) {
        return of(DataTypes.LONG, Long.toString(value));
    }

    /**
     * Convenience factory for a {@code FLOAT} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.FLOAT, Float.toString(value))}.
     *
     * @param value the float value
     * @return the stage
     */
    public static @NotNull LiteralSource<Float> floatVal(float value) {
        return of(DataTypes.FLOAT, Float.toString(value));
    }

    /**
     * Convenience factory for a {@code DOUBLE} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.DOUBLE, Double.toString(value))}.
     *
     * @param value the double value
     * @return the stage
     */
    public static @NotNull LiteralSource<Double> doubleVal(double value) {
        return of(DataTypes.DOUBLE, Double.toString(value));
    }

    /**
     * Convenience factory for a {@code BOOLEAN} value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.BOOLEAN, Boolean.toString(value))}.
     *
     * @param value the boolean value
     * @return the stage
     */
    public static @NotNull LiteralSource<Boolean> booleanVal(boolean value) {
        return of(DataTypes.BOOLEAN, Boolean.toString(value));
    }

    private static @NotNull Object parse(@NotNull DataType<?> type, @NotNull String raw) {
        if (STRING_LIKE.contains(type)) return raw;
        if (type.equals(DataTypes.INT)) return Integer.parseInt(raw.trim());
        if (type.equals(DataTypes.LONG)) return Long.parseLong(raw.trim());
        if (type.equals(DataTypes.FLOAT)) return Float.parseFloat(raw.trim());
        if (type.equals(DataTypes.DOUBLE)) return Double.parseDouble(raw.trim());
        if (type.equals(DataTypes.BOOLEAN)) return Boolean.parseBoolean(raw.trim());
        throw new IllegalArgumentException(
            "LiteralSource does not support outputType " + type + "; wire LiteralSource(RAW_*) -> ParseXxxTransform instead"
        );
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull T execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        return this.value;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.SOURCE_LITERAL;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.outputType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Literal " + this.outputType.label() + " '" + this.rawValue + "'";
    }

}
