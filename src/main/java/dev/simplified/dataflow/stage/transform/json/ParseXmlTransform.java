package dev.simplified.dataflow.stage.transform.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;

/**
 * {@link TransformStage} that parses a {@link DataTypes#RAW_XML} body into a Gson
 * {@link JsonElement} tree.
 * <p>
 * Uses a Jackson {@link XmlMapper} configured so that single-child elements are not
 * wrapped in an intermediate array and mixed-content text is exposed under the key
 * {@value #TEXT_KEY}. The resulting Jackson {@link JsonNode} tree is walked
 * recursively and re-emitted as a Gson tree, preserving primitive node types.
 * <p>
 * Inspired by {@code dev.simplified.client.codec.XmlDecoder} but inlined here so
 * {@code dataflow} does not depend on the {@code client} module.
 */
@StageSpec(
    id = "PARSE_XML",
    displayName = "Parse XML",
    description = "RAW_XML -> JSON_ELEMENT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseXmlTransform implements TransformStage<String, JsonElement> {

    /**
     * Object-key under which Jackson emits mixed-content text when an XML element carries
     * both attributes and text content; matches the common attribute-prefix JSON convention.
     */
    public static final @NotNull String TEXT_KEY = "$";

    private static final @NotNull XmlMapper MAPPER = defaultXmlMapper();

    /**
     * Constructs a parse-XML stage.
     *
     * @return the stage
     */
    public static @NotNull ParseXmlTransform of() {
        return new ParseXmlTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        try {
            return toGsonTree(MAPPER.readTree(input));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse XML body", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.RAW_XML;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as XML";
    }

    private static @NotNull XmlMapper defaultXmlMapper() {
        JacksonXmlModule module = new JacksonXmlModule();
        module.setDefaultUseWrapper(false);
        module.setXMLTextElementName(TEXT_KEY);
        return new XmlMapper(module);
    }

    private static @NotNull JsonElement toGsonTree(@Nullable JsonNode node) {
        if (node == null || node.isNull())
            return JsonNull.INSTANCE;

        if (node.isTextual())
            return new JsonPrimitive(node.asText());

        if (node.isNumber())
            return new JsonPrimitive(node.numberValue());

        if (node.isBoolean())
            return new JsonPrimitive(node.booleanValue());

        if (node.isArray()) {
            JsonArray array = new JsonArray(node.size());

            for (JsonNode child : node)
                array.add(toGsonTree(child));

            return array;
        }

        if (node.isObject()) {
            JsonObject object = new JsonObject();
            Iterator<String> names = node.fieldNames();

            while (names.hasNext()) {
                String name = names.next();
                object.add(name, toGsonTree(node.get(name)));
            }

            return object;
        }

        return new JsonPrimitive(node.asText());
    }

}
