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
 * ({@code STRING}, {@code RAW_*}) or via the matching {@code parseXxx}
 * ({@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE}, {@code BOOLEAN}). Structured
 * types ({@code DOM_NODE}, {@code JSON_*}) are rejected at build time and should be wired
 * via {@code OfSource(RAW_*) -> ParseXxxTransform}.
 *
 * @param <T> the value type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class OfSource<T> implements SourceStage<T> {

    private static final @NotNull Set<DataType<?>> STRING_LIKE = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON, DataTypes.RAW_TEXT
    );

    private final @NotNull DataType<T> outputType;

    private final @NotNull String rawValue;

    private final @NotNull T value;

    /**
     * Constructs an {@code OfSource} that emits {@code value} parsed under {@code outputType}.
     *
     * @param outputType the type to emit
     * @param value the value's serialized form
     * @return the stage
     * @param <T> the value type
     * @throws IllegalArgumentException when {@code outputType} is unsupported or {@code value} cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public static <T> @NotNull OfSource<T> of(@NotNull DataType<T> outputType, @NotNull String value) {
        T parsed = (T) parse(outputType, value);
        return new OfSource<>(outputType, value, parsed);
    }

    /**
     * Reconstructs an {@code OfSource} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull OfSource<?> fromConfig(@NotNull StageConfig cfg) {
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
            "OfSource does not support outputType " + type + "; wire OfSource(RAW_*) -> ParseXxxTransform instead"
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
        return StageKind.SOURCE_OF;
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
