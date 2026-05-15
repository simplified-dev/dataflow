package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * {@link SourceStage} that emits a single literal value parsed from its configuration.
 * Equivalent to {@link java.util.stream.Stream#of(Object)}.
 * <p>
 * Supported {@code outputType}s parse from the configured string verbatim
 * ({@code STRING}, {@code RAW_HTML}, {@code RAW_XML}, {@code RAW_JSON}) or via the matching
 * {@code parseXxx} ({@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE},
 * {@code BOOLEAN}). Structured types ({@code DOM_NODE}, {@code JSON_*}) are rejected at
 * build time and should be wired via {@code LiteralSource(RAW_*) -> ParseXxxTransform}.
 *
 * @param <T> the value type
 */
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
    public static <T> @NotNull LiteralSource<T> of(@NotNull DataType<T> outputType, @NotNull String value) {
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
    public static @NotNull LiteralSource<String> html(@NotNull String body) {
        return of(DataTypes.RAW_HTML, body);
    }

    /**
     * Convenience factory for an XML body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_XML, body)}.
     *
     * @param body the XML body
     * @return the stage
     */
    public static @NotNull LiteralSource<String> xml(@NotNull String body) {
        return of(DataTypes.RAW_XML, body);
    }

    /**
     * Convenience factory for a JSON body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_JSON, body)}.
     *
     * @param body the JSON body
     * @return the stage
     */
    public static @NotNull LiteralSource<String> json(@NotNull String body) {
        return of(DataTypes.RAW_JSON, body);
    }

    /**
     * Convenience factory for a plain string value. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.STRING, body)}.
     *
     * @param body the string value
     * @return the stage
     */
    public static @NotNull LiteralSource<String> text(@NotNull String body) {
        return of(DataTypes.STRING, body);
    }

    /**
     * Reconstructs an {@code LiteralSource} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull LiteralSource<?> fromConfig(@NotNull StageConfig cfg) {
        return of((DataType) cfg.getDataType("outputType"), cfg.getString("value"));
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
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("outputType", this.outputType)
            .string("value", this.rawValue)
            .build();
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
        return "Of " + this.outputType.label() + " '" + this.rawValue + "'";
    }

}
