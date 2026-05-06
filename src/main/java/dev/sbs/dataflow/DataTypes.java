package dev.sbs.dataflow;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
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

    private static final @NotNull Map<String, DataType<?>> BASICS = collectBasics();

    private static @NotNull Map<String, DataType<?>> collectBasics() {
        Map<String, DataType<?>> map = new HashMap<>();
        for (Field f : DataTypes.class.getDeclaredFields()) {
            if (!Modifier.isStatic(f.getModifiers())) continue;
            if (!DataType.class.isAssignableFrom(f.getType())) continue;
            try {
                DataType<?> t = (DataType<?>) f.get(null);
                if (t != null) map.put(t.label(), t);
            } catch (IllegalAccessException ignored) {
                // skip non-accessible
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Looks up a {@link DataType} by its label. Recognises every {@code public static final}
     * field declared in this class plus the {@code List<...>} and {@code Set<...>} forms.
     *
     * @param label the label to look up
     * @return the matching {@link DataType}, or {@code null} when unknown
     */
    public static @Nullable DataType<?> byLabel(@NotNull String label) {
        if (label.startsWith("List<") && label.endsWith(">")) {
            DataType<?> inner = byLabel(label.substring(5, label.length() - 1));
            return inner == null ? null : DataType.list(inner);
        }
        if (label.startsWith("Set<") && label.endsWith(">")) {
            DataType<?> inner = byLabel(label.substring(4, label.length() - 1));
            return inner == null ? null : DataType.set(inner);
        }
        return BASICS.get(label);
    }

}
