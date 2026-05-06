package dev.sbs.dataflow;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

/**
 * Runtime descriptor for a value flowing through a {@link DataPipeline}.
 * <p>
 * Identity is by {@link #label()} alone, not {@link #javaType()}, so that conceptually
 * distinct types backed by the same Java class - {@code RAW_HTML}, {@code RAW_XML},
 * {@code RAW_JSON}, {@code STRING} all over {@link String} - remain distinguishable to the
 * type-chain validator.
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
    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    @EqualsAndHashCode(of = "label")
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
    @EqualsAndHashCode
    final class ListType<E> implements DataType<List<E>> {

        @Getter @Accessors(fluent = true) private final @NotNull DataType<E> element;

        ListType(@NotNull DataType<E> element) {
            this.element = element;
        }

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
    @EqualsAndHashCode
    final class SetType<E> implements DataType<Set<E>> {

        @Getter @Accessors(fluent = true) private final @NotNull DataType<E> element;

        SetType(@NotNull DataType<E> element) {
            this.element = element;
        }

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
