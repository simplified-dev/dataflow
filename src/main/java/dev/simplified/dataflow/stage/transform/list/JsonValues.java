package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import dev.simplified.annotations.UtilityClass;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Node;

/**
 * Writes the elements of a list stage into the JSON objects it builds.
 * <p>
 * A value is written the way an {@link ObjectBuildTransform} output is, through
 * {@link PipelineGson#gson()}, except that a {@link JsonElement} is copied rather than shared, so a
 * built object never aliases a tree another stage holds.
 */
@UtilityClass
final class JsonValues {

    /**
     * Refuses a type whose values cannot be written as JSON.
     * <p>
     * Gson cannot write a jsoup node, and {@code NONE} carries no value, so a type whose element
     * type at any depth is backed by a jsoup {@link Node} or by {@link Void} is refused.
     *
     * @param type the declared type
     * @param stage simple name of the stage whose factory checks the type
     * @param slot the config slot declaring the type
     * @throws IllegalArgumentException when {@code type} cannot be written as JSON
     */
    static void requireWritable(@NotNull DataType<?> type, @NotNull String stage, @NotNull String slot) {
        Class<?> leaf = leafType(type);

        if (Node.class.isAssignableFrom(leaf) || leaf == Void.class)
            throw new IllegalArgumentException(String.format(
                "Invalid %s %s: '%s' cannot be written as JSON", stage, slot, type.label()
            ));
    }

    /**
     * Tells whether a value is absent, a Java {@code null} or a JSON {@code null}.
     *
     * @param value the value
     * @return {@code true} when {@code value} is absent
     */
    static boolean isNull(@Nullable Object value) {
        return value == null || (value instanceof JsonElement element && element.isJsonNull());
    }

    /**
     * Writes a value as a JSON tree of its own.
     *
     * @param value the value, never absent
     * @return a copy of {@code value} when it is a {@link JsonElement}, else its Gson tree
     */
    static @NotNull JsonElement toJson(@NotNull Object value) {
        return value instanceof JsonElement element ? element.deepCopy() : PipelineGson.gson().toJsonTree(value);
    }

    private static @NotNull Class<?> leafType(@NotNull DataType<?> type) {
        return switch (type) {
            case DataType.ListType<?> list -> leafType(list.element());
            case DataType.SetType<?> set -> leafType(set.element());
            case DataType.Basic<?> basic -> basic.javaType();
        };
    }

}
