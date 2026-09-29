package dev.simplified.dataflow;

import com.google.gson.JsonElement;
import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

/**
 * Runtime descriptor for a value flowing through a {@link DataPipeline}.
 * <p>
 * Identity is by {@link #label()} alone, not {@link #javaType()}, so that conceptually
 * distinct types backed by the same Java class - {@code RAW_HTML}, {@code RAW_XML},
 * {@code RAW_JSON}, {@code STRING} all over {@link String} - remain distinguishable to the
 * type-chain validator. Which types a consumer accepts is {@link #isAssignableTo(DataType)}.
 *
 * @param <T> the runtime Java type carried by values of this {@code DataType}
 */
public sealed interface DataType<T> permits DataType.Basic, DataType.ListType, DataType.SetType {

    /**
     * Java class of values described by this type.
     *
     * @return the runtime java class
     */
    @NotNull Class<T> javaType();

    /**
     * Stable identifier used for equality, serialisation, and UI rendering.
     *
     * @return the label
     */
    @NotNull String label();

    /**
     * Returns whether a value of this type satisfies a consumer that expects {@code target}.
     * <p>
     * Every type check a pipeline makes goes through here - one stage's output against the next
     * stage's input, a body's seed and last output, an operand's output, a pipeline narrowed by
     * {@link DataPipeline#expectOutput(DataType)}. A type is assignable to:
     * <ul>
     *   <li><b>itself</b> - an equal type, by {@link #label()}</li>
     *   <li><b>{@code JSON_ELEMENT}</b> - from {@code JSON_OBJECT} and {@code JSON_ARRAY}, whose
     *   values are Gson {@link JsonElement}s already, so nothing is converted</li>
     *   <li><b>a list or set of a wider element</b> - a {@code List} to a {@code List}, a
     *   {@code Set} to a {@code Set}, when its element type is assignable to the other's; a
     *   pipeline never mutates a collection it is handed, which is what makes reading it as one
     *   of a wider element safe</li>
     * </ul>
     * Nothing else widens: identity is by label, so {@code RAW_HTML} is not assignable to
     * {@code STRING}, and no numeric type is assignable to another.
     *
     * @param target the type the consumer expects
     * @return {@code true} when a value of this type may flow where {@code target} is expected
     */
    default boolean isAssignableTo(@NotNull DataType<?> target) {
        if (this.equals(target)) return true;

        return switch (this) {
            case Basic<?> basic -> target.equals(DataTypes.JSON_ELEMENT)
                && (basic.equals(DataTypes.JSON_OBJECT) || basic.equals(DataTypes.JSON_ARRAY));
            case ListType<?> list -> target instanceof ListType<?> other && list.element().isAssignableTo(other.element());
            case SetType<?> set -> target instanceof SetType<?> other && set.element().isAssignableTo(other.element());
        };
    }

    /**
     * Constructs a list type whose elements are the given element type.
     *
     * @param element the element type
     * @return a {@code ListType} over {@code element}
     * @param <E> element type
     */
    static <E> @NotNull ListType<E> list(@NotNull DataType<E> element) {
        return new ListType<>(element);
    }

    /**
     * Constructs a set type whose elements are the given element type.
     *
     * @param element the element type
     * @return a {@code SetType} over {@code element}
     * @param <E> element type
     */
    static <E> @NotNull SetType<E> set(@NotNull DataType<E> element) {
        return new SetType<>(element);
    }

    /**
     * Leaf {@link DataType} backed by a single Java class.
     *
     * @param <T> the runtime java type
     */
    @Getter(style = NamingStyle.FLUENT)
    @EqualsAndHashCode(of = "label")
    @RequiredArgsConstructor
    final class Basic<T> implements DataType<T> {

        private final @NotNull Class<T> javaType;
        private final @NotNull String label;

        @Override
        public @NotNull String toString() {
            return this.label;
        }

    }

    /**
     * Parameterised {@link DataType} representing {@link List} of an element type.
     *
     * @param <E> element type
     */
    @Getter(style = NamingStyle.FLUENT)
    @EqualsAndHashCode
    @RequiredArgsConstructor
    final class ListType<E> implements DataType<List<E>> {

        private final @NotNull DataType<E> element;

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public @NotNull Class<List<E>> javaType() {
            return (Class) List.class;
        }

        @Override
        public @NotNull String label() {
            return "List<" + this.element.label() + ">";
        }

        @Override
        public @NotNull String toString() {
            return this.label();
        }

    }

    /**
     * Parameterised {@link DataType} representing {@link Set} of an element type.
     *
     * @param <E> element type
     */
    @Getter(style = NamingStyle.FLUENT)
    @EqualsAndHashCode
    @RequiredArgsConstructor
    final class SetType<E> implements DataType<Set<E>> {

        private final @NotNull DataType<E> element;

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public @NotNull Class<Set<E>> javaType() {
            return (Class) Set.class;
        }

        @Override
        public @NotNull String label() {
            return "Set<" + this.element.label() + ">";
        }

        @Override
        public @NotNull String toString() {
            return this.label();
        }

    }

}
