package dev.sbs.dataflow.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Converts an XML body into a Gson {@link JsonElement} tree by routing it through Jackson's
 * {@link XmlMapper} (parses the XML structure) and re-emitting the resulting node tree as
 * JSON which Gson then re-parses.
 * <p>
 * The two-hop approach is intentionally simple: it lets every downstream stage operate on
 * the unified Gson element model without {@code dataflow} having to ship its own XML parser
 * or expose Jackson types in its API surface. Inspired by the bridge in
 * {@code dev.simplified.client.codec.XmlDecoder} but reimplemented here so {@code dataflow}
 * does not depend on the {@code client} module.
 */
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class XmlBridge {

    private static final @NotNull XmlMapper MAPPER = new XmlMapper();

    /**
     * Parses an XML body into a {@link JsonElement} tree.
     *
     * @param xml the XML body
     * @return the parsed tree
     * @throws IllegalArgumentException if the XML is malformed
     */
    public static @NotNull JsonElement parse(@NotNull String xml) {
        try {
            JsonNode node = MAPPER.readTree(xml);
            return JsonParser.parseString(node.toString());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse XML body", e);
        }
    }

}
