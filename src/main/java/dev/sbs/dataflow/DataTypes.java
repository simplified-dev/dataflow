package dev.sbs.dataflow;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Element;

import java.util.Map;

/**
 * Catalog of well-known {@link DataType} instances used by the built-in stage catalog.
 * <p>
 * Identity is by label, so values declared here must have unique label strings.
 */
@UtilityClass
public final class DataTypes {

    /** Sentinel input type for source stages, which consume nothing. */
    public static final @NotNull DataType<Void> NONE = new DataType.Basic<>(Void.class, "NONE");

    /** Raw HTML document body, not yet parsed. */
    public static final @NotNull DataType<String> RAW_HTML = new DataType.Basic<>(String.class, "RAW_HTML");

    /** Raw XML document body, not yet parsed. */
    public static final @NotNull DataType<String> RAW_XML = new DataType.Basic<>(String.class, "RAW_XML");

    /** Raw JSON document body, not yet parsed. */
    public static final @NotNull DataType<String> RAW_JSON = new DataType.Basic<>(String.class, "RAW_JSON");

    /** Raw plain-text body, not yet parsed. */
    public static final @NotNull DataType<String> RAW_TEXT = new DataType.Basic<>(String.class, "RAW_TEXT");

    /** General-purpose UTF-8 string value. */
    public static final @NotNull DataType<String> STRING = new DataType.Basic<>(String.class, "STRING");

    /** 32-bit signed integer value. */
    public static final @NotNull DataType<Integer> INT = new DataType.Basic<>(Integer.class, "INT");

    /** 64-bit signed integer value. */
    public static final @NotNull DataType<Long> LONG = new DataType.Basic<>(Long.class, "LONG");

    /** 64-bit IEEE-754 floating-point value. */
    public static final @NotNull DataType<Double> DOUBLE = new DataType.Basic<>(Double.class, "DOUBLE");

    /** Boolean value. */
    public static final @NotNull DataType<Boolean> BOOLEAN = new DataType.Basic<>(Boolean.class, "BOOLEAN");

    /** Single jsoup {@link Element}, the parsed-HTML pivot value. */
    public static final @NotNull DataType<Element> DOM_NODE = new DataType.Basic<>(Element.class, "DOM_NODE");

    /** Generic Gson {@link JsonElement}. */
    public static final @NotNull DataType<JsonElement> JSON_ELEMENT = new DataType.Basic<>(JsonElement.class, "JSON_ELEMENT");

    /** Gson {@link JsonObject}. */
    public static final @NotNull DataType<JsonObject> JSON_OBJECT = new DataType.Basic<>(JsonObject.class, "JSON_OBJECT");

    /** Gson {@link JsonArray}. */
    public static final @NotNull DataType<JsonArray> JSON_ARRAY = new DataType.Basic<>(JsonArray.class, "JSON_ARRAY");

    /** Output type of {@link dev.sbs.dataflow.stage.BranchStage}, a map of named sub-pipeline results. */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static final @NotNull DataType<Map<String, Object>> BRANCH_OUTPUT =
        new DataType.Basic<>((Class) Map.class, "BRANCH_OUTPUT");

}
