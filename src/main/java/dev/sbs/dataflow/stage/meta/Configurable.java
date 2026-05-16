package dev.sbs.dataflow.stage.meta;

import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.StageConfig;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a stage-factory parameter as a configurable wire slot.
 * <p>
 * The framework derives the slot's {@link FieldSpec} entirely from the parameter:
 * <ul>
 *   <li><b>name</b> - the Java parameter name (requires {@code -parameters} compile flag),
 *       or {@link #name()} when explicitly overridden</li>
 *   <li><b>type</b> - {@link FieldSpec.Type} discriminator inferred from the Java parameter type
 *       (e.g. {@code String} -> {@code STRING}, {@code DataType<?>} -> {@code DATA_TYPE},
 *       {@code Chain} -> {@code SUB_PIPELINE})</li>
 *   <li><b>label</b> - {@link #label()}</li>
 *   <li><b>placeholder</b> - {@link #placeholder()}</li>
 *   <li><b>optional</b> - {@link #optional()}; when {@code true} the framework passes
 *       {@code null} if the slot is absent from the populated {@link StageConfig}</li>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Configurable {

    /**
     * Human-friendly title shown next to the field's input.
     *
     * @return the label
     */
    @NotNull String label();

    /**
     * Example value shown inside an empty input. Also used by tests to build default
     * configurations for round-trip checks.
     *
     * @return the placeholder, or empty when no hint applies
     */
    String placeholder() default "";

    /**
     * Whether this slot may be absent from the populated {@link StageConfig}. When
     * {@code true}, the framework passes {@code null} to the factory parameter; the
     * factory body is expected to handle the absent case.
     *
     * @return {@code true} when the slot is optional
     */
    boolean optional() default false;

    /**
     * Overrides the wire key derived from the Java parameter name. Use when the desired
     * JSON property name is not a valid Java identifier, or when the parameter name was
     * not preserved at compile time.
     *
     * @return the override name, or empty to use the Java parameter name
     */
    @NotNull String name() default "";

}
