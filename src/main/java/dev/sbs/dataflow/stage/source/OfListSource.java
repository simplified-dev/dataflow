package dev.sbs.dataflow.stage.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@link SourceStage} that emits a literal list parsed from a JSON-array string at build
 * time. Equivalent to {@link java.util.stream.Stream#of(Object[])} for the supported
 * element types.
 * <p>
 * Same supported element types as {@link OfSource}: {@code STRING} / {@code RAW_*},
 * {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE}, {@code BOOLEAN}. Structured
 * types are rejected at build time.
 *
 * @param <T> the element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class OfListSource<T> implements SourceStage<List<T>> {

    private static final @NotNull Set<DataType<?>> STRING_LIKE = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON, DataTypes.RAW_TEXT
    );

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> outputType;

    private final @NotNull String rawValue;

    private final @NotNull ConcurrentList<T> values;

    /**
     * Constructs an {@code OfListSource} that emits the parsed JSON array.
     *
     * @param elementType element type of the emitted list
     * @param rawValue JSON array, e.g. {@code "[\"a\",\"b\"]"} or {@code "[1,2,3]"}
     * @return the stage
     * @param <T> the element type
     * @throws IllegalArgumentException when {@code elementType} is unsupported, the value is not a JSON array, or an element cannot be parsed
     */
    public static <T> @NotNull OfListSource<T> of(@NotNull DataType<T> elementType, @NotNull String rawValue) {
        ConcurrentList<T> parsed = Concurrent.newUnmodifiableList(parseArray(elementType, rawValue));
        return new OfListSource<>(elementType, DataType.list(elementType), rawValue, parsed);
    }

    /**
     * Reconstructs an {@code OfListSource} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull OfListSource<?> fromConfig(@NotNull StageConfig cfg) {
        return of((DataType) cfg.getDataType("elementType"), cfg.getString("value"));
    }

    private static final @NotNull Set<DataType<?>> SUPPORTED_ELEMENT_TYPES = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON, DataTypes.RAW_TEXT,
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.BOOLEAN
    );

    @SuppressWarnings("unchecked")
    private static <T> @NotNull List<T> parseArray(@NotNull DataType<T> elementType, @NotNull String raw) {
        if (!SUPPORTED_ELEMENT_TYPES.contains(elementType))
            throw new IllegalArgumentException(
                "OfListSource does not support elementType " + elementType
                    + "; wire OfListSource(RAW_*) -> MapTransform(... ParseXxx) instead"
            );

        JsonElement root;
        try {
            JsonReader reader = new JsonReader(new StringReader(raw));
            reader.setStrictness(Strictness.STRICT);
            root = JsonParser.parseReader(reader);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("OfListSource value is not valid JSON: " + raw, e);
        }

        if (!root.isJsonArray())
            throw new IllegalArgumentException("OfListSource value must be a JSON array but was: " + raw);

        JsonArray array = root.getAsJsonArray();
        List<T> result = new ArrayList<>(array.size());

        for (JsonElement el : array)
            result.add((T) parseElement(elementType, el));

        return result;
    }

    private static @NotNull Object parseElement(@NotNull DataType<?> elementType, @NotNull JsonElement el) {
        if (STRING_LIKE.contains(elementType)) return el.getAsString();
        if (elementType.equals(DataTypes.INT)) return el.getAsInt();
        if (elementType.equals(DataTypes.LONG)) return el.getAsLong();
        if (elementType.equals(DataTypes.FLOAT)) return el.getAsFloat();
        if (elementType.equals(DataTypes.DOUBLE)) return el.getAsDouble();
        if (elementType.equals(DataTypes.BOOLEAN)) return el.getAsBoolean();
        throw new IllegalArgumentException(
            "OfListSource does not support elementType " + elementType
        );
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .string("value", this.rawValue)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        return this.values;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.SOURCE_OF_LIST;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.outputType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "OfList " + this.elementType.label() + " (" + this.values.size() + ")";
    }

}
