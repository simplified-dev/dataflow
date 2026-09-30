package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@link TransformStage} that deserialises a {@link JsonElement} into an instance of an
 * arbitrary target type via Gson.
 * <p>
 * The target type is a {@link DataType.Basic Basic} {@link DataType}, so a concrete
 * {@code Class<T>} is available, or a {@link DataType.ListType List} of one. A Basic target
 * converts the whole input. A {@code List<X>} target reads a JSON array into a list in array
 * order, converting each element to {@code X}; an element that is JSON null or does not convert
 * is dropped, and an input that is not an array rejects with {@code null}. Set types and lists
 * of non-Basic types are rejected at build time, and so are a list of
 * {@link DataTypes#DOM_NODE DOM_NODE} or {@link DataTypes#NONE NONE}, which Gson cannot build
 * from JSON, and a list output over a {@link DataTypes#JSON_OBJECT JSON_OBJECT} input, which
 * never holds an array. Use {@link DataTypes#register(DataType)} to make custom POJO types
 * resolvable by the wire-format deserialiser.
 * <p>
 * Input may be tagged {@link DataTypes#JSON_ELEMENT}, {@link DataTypes#JSON_OBJECT} or
 * {@link DataTypes#JSON_ARRAY}; the underlying runtime value is always a {@link JsonElement}.
 *
 * @param <I> input element tag (one of the Gson types)
 * @param <T> deserialisation target type
 */
@StageSpec(
    id = "TRANSFORM_JSON_DESERIALIZE",
    displayName = "Json deserialize",
    description = "JSON_* -> T",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DeserializeTransform<I extends JsonElement, T> implements TransformStage<I, T> {

    private static final @NotNull Set<DataType<?>> SUPPORTED_INPUT_TYPES = Set.of(
        DataTypes.JSON_ELEMENT, DataTypes.JSON_OBJECT, DataTypes.JSON_ARRAY
    );

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<T> outputType;

    /**
     * Element type of a {@code List} output, or {@code null} when the output is Basic.
     */
    private final @Nullable DataType<?> listElement;

    /**
     * Constructs a JSON deserialisation stage with input tag {@link DataTypes#JSON_ELEMENT}.
     *
     * @param outputType the target {@link DataType}; a Basic type or a {@code List} of one
     * @return the stage
     * @param <T> deserialisation target type
     * @throws IllegalArgumentException when {@code outputType} is neither a {@link DataType.Basic}
     *         nor a {@code List} of one, or is a {@code List} of an element Gson cannot build from
     *         JSON
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> @NotNull DeserializeTransform<JsonElement, T> of(@NotNull DataType<T> outputType) {
        return (DeserializeTransform) of(DataTypes.JSON_ELEMENT, outputType);
    }

    /**
     * Constructs a JSON deserialisation stage with an explicit input tag.
     *
     * @param inputType the Gson-typed input tag (one of {@code JSON_ELEMENT}, {@code JSON_OBJECT},
     *                  {@code JSON_ARRAY})
     * @param outputType the target {@link DataType}; a Basic type or a {@code List} of one
     * @return the stage
     * @param <I> input element tag
     * @param <T> deserialisation target type
     * @throws IllegalArgumentException when {@code outputType} is neither a {@link DataType.Basic}
     *         nor a {@code List} of one, {@code inputType} is not a recognised Gson type, or
     *         {@code outputType} is a {@code List} of an element Gson cannot build from JSON or
     *         over a {@code JSON_OBJECT} input, which never holds an array
     */
    public static <I extends JsonElement, T> @NotNull DeserializeTransform<I, T> of(
        @Configurable(label = "Input type", placeholder = "JSON_ELEMENT")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Output type", placeholder = "STRING")
        @NotNull DataType<T> outputType
    ) {
        if (!SUPPORTED_INPUT_TYPES.contains(inputType))
            throw new IllegalArgumentException(
                "DeserializeTransform input must be one of " + SUPPORTED_INPUT_TYPES + " but got " + inputType.label()
            );
        if (outputType instanceof DataType.ListType<?> list && list.element() instanceof DataType.Basic<?> element) {
            requireListReadable(inputType, outputType, element);
            return new DeserializeTransform<>(inputType, outputType, element);
        }
        if (!(outputType instanceof DataType.Basic<T>))
            throw new IllegalArgumentException(
                "DeserializeTransform requires a Basic output DataType or a List of one but got " + outputType.label()
            );
        return new DeserializeTransform<>(inputType, outputType, null);
    }

    /**
     * Refuses a {@code List} output that no input could ever fill.
     * <p>
     * Gson cannot build a jsoup {@link Node} or a {@link Void} from JSON, so every element of such a
     * list would fail to convert and the list would always be empty; a {@code JSON_OBJECT} input
     * never holds an array, so the stage would always reject.
     *
     * @param inputType the input tag
     * @param outputType the {@code List} output type
     * @param element the element type of {@code outputType}
     * @throws IllegalArgumentException when {@code element} cannot be built from JSON, or
     *         {@code inputType} is {@code JSON_OBJECT}
     */
    private static void requireListReadable(
        @NotNull DataType<?> inputType,
        @NotNull DataType<?> outputType,
        @NotNull DataType.Basic<?> element
    ) {
        Class<?> javaType = element.javaType();

        if (Node.class.isAssignableFrom(javaType) || javaType == Void.class) {
            throw new IllegalArgumentException(String.format(
                "Invalid DeserializeTransform outputType: '%s' elements cannot be built from JSON", element.label()
            ));
        }

        if (DataTypes.JSON_OBJECT.equals(inputType)) {
            throw new IllegalArgumentException(String.format(
                "Invalid DeserializeTransform outputType: a '%s' input never holds an array, so it cannot yield '%s'",
                inputType.label(), outputType.label()
            ));
        }
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null || input.isJsonNull()) return null;
        if (this.listElement == null) return PipelineGson.gson().fromJson(input, this.outputType.javaType());
        if (!input.isJsonArray()) return null;
        return (T) readList(input.getAsJsonArray(), this.listElement);
    }

    private static @NotNull List<Object> readList(@NotNull JsonArray array, @NotNull DataType<?> element) {
        List<Object> values = new ArrayList<>(array.size());

        for (JsonElement item : array) {
            if (item.isJsonNull()) continue;

            try {
                Object value = PipelineGson.gson().fromJson(item, element.javaType());
                if (value != null) values.add(value);
            } catch (JsonParseException | NumberFormatException ignored) { }
        }

        return Concurrent.newUnmodifiableList(values);
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Json deserialise " + this.inputType.label() + " -> " + this.outputType.label();
    }

}
