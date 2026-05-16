package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.serde.PipelineGson;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * {@link TransformStage} that deserialises a {@link JsonElement} into an instance of an
 * arbitrary target type via Gson.
 * <p>
 * The target type must be a {@link DataType.Basic Basic} {@link DataType} so a concrete
 * {@code Class<T>} is available; parameterised list / set types are rejected at build
 * time. Use {@link DataTypes#register(DataType)} to make custom POJO types resolvable by
 * the wire-format deserialiser.
 * <p>
 * Input may be tagged {@link DataTypes#JSON_ELEMENT}, {@link DataTypes#JSON_OBJECT} or
 * {@link DataTypes#JSON_ARRAY}; the underlying runtime value is always a {@link JsonElement}.
 *
 * @param <I> input element tag (one of the Gson types)
 * @param <T> deserialisation target type
 */
@StageSpec(
    id = "TRANSFORM_GSON_DESERIALIZE",
    displayName = "Gson deserialize",
    description = "JSON_* -> T",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GsonDeserializeTransform<I extends JsonElement, T> implements TransformStage<I, T> {

    private static final @NotNull Set<DataType<?>> SUPPORTED_INPUT_TYPES = Set.of(
        DataTypes.JSON_ELEMENT, DataTypes.JSON_OBJECT, DataTypes.JSON_ARRAY
    );

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<T> outputType;

    /**
     * Constructs a Gson deserialisation stage with input tag {@link DataTypes#JSON_ELEMENT}.
     *
     * @param outputType the target {@link DataType}; must wrap a concrete {@link Class}
     * @return the stage
     * @param <T> deserialisation target type
     * @throws IllegalArgumentException when {@code outputType} is not a {@link DataType.Basic}
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> @NotNull GsonDeserializeTransform<JsonElement, T> of(@NotNull DataType<T> outputType) {
        return (GsonDeserializeTransform) of(DataTypes.JSON_ELEMENT, outputType);
    }

    /**
     * Constructs a Gson deserialisation stage with an explicit input tag.
     *
     * @param inputType the Gson-typed input tag (one of {@code JSON_ELEMENT}, {@code JSON_OBJECT},
     *                  {@code JSON_ARRAY})
     * @param outputType the target {@link DataType}; must wrap a concrete {@link Class}
     * @return the stage
     * @param <I> input element tag
     * @param <T> deserialisation target type
     * @throws IllegalArgumentException when {@code outputType} is not a {@link DataType.Basic}
     *         or {@code inputType} is not a recognised Gson type
     */
    public static <I extends JsonElement, T> @NotNull GsonDeserializeTransform<I, T> of(
        @Configurable(label = "Input type", placeholder = "JSON_ELEMENT")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Output type", placeholder = "STRING")
        @NotNull DataType<T> outputType
    ) {
        if (!SUPPORTED_INPUT_TYPES.contains(inputType))
            throw new IllegalArgumentException(
                "GsonDeserializeTransform input must be one of " + SUPPORTED_INPUT_TYPES + " but got " + inputType.label()
            );
        if (!(outputType instanceof DataType.Basic<T>))
            throw new IllegalArgumentException(
                "GsonDeserializeTransform requires a Basic output DataType but got " + outputType.label()
            );
        return new GsonDeserializeTransform<>(inputType, outputType);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null || input.isJsonNull()) return null;
        return PipelineGson.gson().fromJson(input, this.outputType.javaType());
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Gson deserialise " + this.inputType.label() + " -> " + this.outputType.label();
    }

}
