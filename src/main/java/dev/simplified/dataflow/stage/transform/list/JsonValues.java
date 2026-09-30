package dev.simplified.dataflow.stage.transform.list;

import com.google.gson.JsonElement;
import dev.simplified.annotations.UtilityClass;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Node;

import java.util.Collection;
import java.util.Map;

/**
 * Helpers that write the elements of a list stage into the JSON objects it builds.
 * <p>
 * A value is written the way an {@link ObjectBuildTransform} output is, through
 * {@link PipelineGson#gson()}, except that a {@link JsonElement} is copied rather than shared, so a
 * built object never aliases a tree another stage holds, and a value JSON cannot hold - a
 * {@code NaN} or infinite {@link Double} or {@link Float} - is rejected as absent rather than
 * failing the run, the way the arithmetic stages reject a non-finite result.
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

        if (Node.class.isAssignableFrom(leaf) || leaf == Void.class) {
            throw new IllegalArgumentException(String.format(
                "Invalid %s %s: '%s' cannot be written as JSON", stage, slot, type.label()
            ));
        }
    }

    /**
     * Writes a value as a JSON tree of its own.
     * <p>
     * A value is absent when it is a Java {@code null} or a JSON {@code null}, and also when JSON
     * cannot hold it: a {@code NaN} or infinite {@link Double} or {@link Float}, or a list, set or
     * map holding one at any depth.
     *
     * @param value the value
     * @return a copy of {@code value} when it is a {@link JsonElement}, else its Gson tree, or
     *         {@code null} when {@code value} is absent
     */
    static @Nullable JsonElement toJson(@Nullable Object value) {
        if (value == null || !hasJsonForm(value)) return null;
        if (value instanceof JsonElement element) return element.isJsonNull() ? null : element.deepCopy();
        return PipelineGson.gson().toJsonTree(value);
    }

    private static boolean hasJsonForm(@Nullable Object value) {
        return switch (value) {
            case null -> true;
            case Double number -> Double.isFinite(number);
            case Float number -> Float.isFinite(number);
            case Collection<?> members -> members.stream().allMatch(JsonValues::hasJsonForm);
            case Map<?, ?> entries -> entries.values().stream().allMatch(JsonValues::hasJsonForm);
            default -> true;
        };
    }

    private static @NotNull Class<?> leafType(@NotNull DataType<?> type) {
        return switch (type) {
            case DataType.ListType<?> list -> leafType(list.element());
            case DataType.SetType<?> set -> leafType(set.element());
            case DataType.Basic<?> basic -> basic.javaType();
        };
    }

}
