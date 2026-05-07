package dev.sbs.dataflow.stage;

import org.jetbrains.annotations.NotNull;

/**
 * Declares one slot in a {@link StageKind}'s configuration schema.
 *
 * @param name the field name, also the JSON property key and the Modal text-input identifier
 * @param type the slot's primitive type
 * @param label human-friendly title shown next to the field's input
 * @param placeholder example value shown inside an empty input
 */
public record FieldSpec(
    @NotNull String name,
    @NotNull FieldType type,
    @NotNull String label,
    @NotNull String placeholder
) {

    /**
     * Convenience factory for a field with no placeholder hint.
     *
     * @param name the field name
     * @param type the field type
     * @param label the human-friendly label
     * @return a {@code FieldSpec} with empty placeholder
     */
    public static @NotNull FieldSpec of(@NotNull String name, @NotNull FieldType type, @NotNull String label) {
        return new FieldSpec(name, type, label, "");
    }

}
