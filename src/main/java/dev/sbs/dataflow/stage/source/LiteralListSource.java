package dev.sbs.dataflow.stage.source;

import com.google.gson.Gson;
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
import java.util.stream.Stream;
import java.util.Set;

/**
 * {@link SourceStage} that emits a literal list parsed from a JSON-array string at build
 * time. Equivalent to {@link Stream#of(Object[])} for the supported
 * element types.
 * <p>
 * Same supported element types as {@link LiteralSource}: {@code STRING} / {@code RAW_*},
 * {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE}, {@code BOOLEAN}. Structured
 * types are rejected at build time.
 *
 * @param <T> the element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LiteralListSource<T> implements SourceStage<List<T>> {

    private static final @NotNull Set<DataType<?>> STRING_LIKE = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> outputType;

    private final @NotNull String rawValue;

    private final @NotNull ConcurrentList<T> values;

    /**
     * Constructs an {@code LiteralListSource} that emits the parsed JSON array.
     *
     * @param elementType element type of the emitted list
     * @param rawValue JSON array, e.g. {@code "[\"a\",\"b\"]"} or {@code "[1,2,3]"}
     * @return the stage
     * @param <T> the element type
     * @throws IllegalArgumentException when {@code elementType} is unsupported, the value is not a JSON array, or an element cannot be parsed
     */
    public static <T> @NotNull LiteralListSource<T> of(@NotNull DataType<T> elementType, @NotNull String rawValue) {
        ConcurrentList<T> parsed = Concurrent.newUnmodifiableList(parseArray(elementType, rawValue));
        return new LiteralListSource<>(elementType, DataType.list(elementType), rawValue, parsed);
    }

    /**
     * Convenience factory for a list of {@link String} values. Each value is escaped into
     * the JSON array via {@link Gson}.
     *
     * @param values the string values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<String> strings(@NotNull String... values) {
        return of(DataTypes.STRING, new com.google.gson.Gson().toJson(values));
    }

    /**
     * Convenience factory for a list of {@code INT} values.
     *
     * @param values the int values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<Integer> integers(int... values) {
        return of(DataTypes.INT, toJsonArray(values));
    }

    /**
     * Convenience factory for a list of {@code LONG} values.
     *
     * @param values the long values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<Long> longs(long... values) {
        return of(DataTypes.LONG, toJsonArray(values));
    }

    /**
     * Convenience factory for a list of {@code FLOAT} values.
     *
     * @param values the float values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<Float> floats(float... values) {
        return of(DataTypes.FLOAT, toJsonArray(values));
    }

    /**
     * Convenience factory for a list of {@code DOUBLE} values.
     *
     * @param values the double values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<Double> doubles(double... values) {
        return of(DataTypes.DOUBLE, toJsonArray(values));
    }

    /**
     * Convenience factory for a list of {@code BOOLEAN} values.
     *
     * @param values the boolean values to emit
     * @return the stage
     */
    public static @NotNull LiteralListSource<Boolean> booleans(boolean... values) {
        return of(DataTypes.BOOLEAN, toJsonArray(values));
    }

    private static @NotNull String toJsonArray(int[] values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        return sb.append(']').toString();
    }

    private static @NotNull String toJsonArray(long[] values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        return sb.append(']').toString();
    }

    private static @NotNull String toJsonArray(float[] values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        return sb.append(']').toString();
    }

    private static @NotNull String toJsonArray(double[] values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        return sb.append(']').toString();
    }

    private static @NotNull String toJsonArray(boolean[] values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        return sb.append(']').toString();
    }

    /**
     * Reconstructs an {@code LiteralListSource} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull LiteralListSource<?> fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getDataType("elementType"), cfg.getString("value"));
    }

    private static final @NotNull Set<DataType<?>> SUPPORTED_ELEMENT_TYPES = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON,
        DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.BOOLEAN
    );

    @SuppressWarnings("unchecked")
    private static <T> @NotNull List<T> parseArray(@NotNull DataType<T> elementType, @NotNull String raw) {
        if (!SUPPORTED_ELEMENT_TYPES.contains(elementType))
            throw new IllegalArgumentException(
                "LiteralListSource does not support elementType " + elementType
                    + "; wire LiteralListSource(RAW_*) -> MapTransform(... ParseXxx) instead"
            );

        JsonElement root;
        try {
            JsonReader reader = new JsonReader(new StringReader(raw));
            reader.setStrictness(Strictness.STRICT);
            root = JsonParser.parseReader(reader);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("LiteralListSource value is not valid JSON: " + raw, e);
        }

        if (!root.isJsonArray())
            throw new IllegalArgumentException("LiteralListSource value must be a JSON array but was: " + raw);

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
            "LiteralListSource does not support elementType " + elementType
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
        return StageKind.SOURCE_LITERAL_LIST;
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
