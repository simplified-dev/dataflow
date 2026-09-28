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

import java.util.Set;

/**
 * {@link TransformStage} that retypes a {@code STRING} as {@code RAW_HTML}, {@code RAW_XML} or
 * {@code RAW_JSON}, the counterpart of {@link ToStringTransform}.
 * <p>
 * The value passes through unchanged and nothing is parsed, so a text cut out of a larger body
 * can reach the parse stage for its format. A malformed body fails in that parse stage, as it
 * would coming from a source.
 */
@StageSpec(
    id = "TRANSFORM_TO_RAW",
    displayName = "To raw",
    description = "STRING -> RAW_*",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ToRawTransform implements TransformStage<String, String> {

    private static final @NotNull Set<DataType<?>> SUPPORTED_OUTPUT_TYPES = Set.of(
        DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    private final @NotNull DataType<String> outputType;

    /**
     * Constructs a to-raw stage that tags its input as {@code outputType}.
     *
     * @param outputType the raw type to tag the input as, one of {@code RAW_HTML},
     *                   {@code RAW_XML} and {@code RAW_JSON}
     * @return the stage
     * @throws IllegalArgumentException when {@code outputType} is not one of the raw types
     */
    public static @NotNull ToRawTransform of(
        @Configurable(label = "Output type (RAW_HTML / RAW_XML / RAW_JSON)", placeholder = "RAW_HTML")
        @NotNull DataType<String> outputType
    ) {
        if (!SUPPORTED_OUTPUT_TYPES.contains(outputType)) {
            throw new IllegalArgumentException(
                "ToRawTransform supports " + SUPPORTED_OUTPUT_TYPES + " but got " + outputType.label()
            );
        }

        return new ToRawTransform(outputType);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "To raw " + this.outputType.label();
    }

}
