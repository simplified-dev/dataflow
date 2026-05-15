package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Typed name-to-value bag holding one stage's configuration.
 * <p>
 * {@link Stage#config()} returns this; {@link StageKind#factory()} consumes it.
 * Field types match the stage's {@link StageKind#schema()} - getter calls assume the caller
 * knows the schema (or handles {@code null} when a field is absent).
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StageConfig {

    /**
     * Pairs a named sub-pipeline with the {@link DataType} its body must produce. Carried
     * inside {@link FieldType#TYPED_SUB_PIPELINES_MAP} values.
     *
     * @param outputType expected output type of the body's last stage
     * @param chain the body stages, in execution order
     */
    public record TypedSubPipeline(
        @NotNull DataType<?> outputType,
        @NotNull ConcurrentList<Stage<?, ?>> chain
    ) {}

    private static final @NotNull StageConfig EMPTY = new StageConfig(Concurrent.newUnmodifiableMap());

    private final @NotNull Map<String, Object> values;

    /**
     * Mutable builder. Order is preserved (insertion order).
     */
    public static final class Builder {
        private final @NotNull LinkedHashMap<String, Object> values = new LinkedHashMap<>();

        private Builder() {}

        public @NotNull Builder string(@NotNull String name, @NotNull String value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder integer(@NotNull String name, int value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder longVal(@NotNull String name, long value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder floatVal(@NotNull String name, float value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder doubleVal(@NotNull String name, double value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder bool(@NotNull String name, boolean value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder dataType(@NotNull String name, @NotNull DataType<?> value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull Builder subPipelines(@NotNull String name, @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> value) {
            this.values.put(name, value);
            return this;
        }

        /**
         * Stores a named sub-pipelines map whose values pair each chain with its declared
         * output {@link DataType}.
         *
         * @param name the field name
         * @param value the typed sub-pipelines map
         * @return this builder
         */
        public @NotNull Builder typedSubPipelines(@NotNull String name, @NotNull Map<String, TypedSubPipeline> value) {
            this.values.put(name, value);
            return this;
        }

        /**
         * Stores a singular sub-pipeline under {@code name}. Used by stages such as map /
         * flatMap / takeWhile that carry exactly one inner chain.
         *
         * @param name the field name
         * @param chain the inner chain
         * @return this builder
         */
        public @NotNull Builder subPipeline(@NotNull String name, @NotNull List<? extends Stage<?, ?>> chain) {
            this.values.put(name, chain);
            return this;
        }

        /**
         * Generic store - inserts the value as-is. Caller is responsible for matching
         * {@link FieldType} expectations declared by the {@link FieldSpec}.
         *
         * @param name the field name
         * @param value the value
         * @return this builder
         */
        public @NotNull Builder put(@NotNull String name, @NotNull Object value) {
            this.values.put(name, value);
            return this;
        }

        public @NotNull StageConfig build() {
            return new StageConfig(Concurrent.adoptMap(this.values).toUnmodifiable());
        }
    }

    /**
     * Creates a fresh {@link Builder}.
     *
     * @return a new builder
     */
    public static @NotNull Builder builder() {
        return new Builder();
    }

    /**
     * Returns the canonical empty configuration. Useful for stages with no config slots.
     *
     * @return an empty configuration
     */
    public static @NotNull StageConfig empty() {
        return EMPTY;
    }

    /** Returns the boolean stored under {@code name}. */
    public boolean getBoolean(@NotNull String name) {
        return (Boolean) this.values.get(name);
    }

    /** Returns the {@link DataType} stored under {@code name}. */
    public @NotNull DataType<?> getDataType(@NotNull String name) {
        return (DataType<?>) this.values.get(name);
    }

    /** Returns the double stored under {@code name}. */
    public double getDouble(@NotNull String name) {
        return (Double) this.values.get(name);
    }

    /** Returns the float stored under {@code name}. */
    public float getFloat(@NotNull String name) {
        return (Float) this.values.get(name);
    }

    /** Returns the int stored under {@code name}. */
    public int getInt(@NotNull String name) {
        return (Integer) this.values.get(name);
    }

    /** Returns the long stored under {@code name}. */
    public long getLong(@NotNull String name) {
        return (Long) this.values.get(name);
    }

    /**
     * Returns the {@link String} stored under {@code name}.
     *
     * @param name the field name
     * @return the stored string
     * @throws ClassCastException when the field is present but not a {@code String}
     * @throws NullPointerException when the field is absent
     */
    public @NotNull String getString(@NotNull String name) {
        return (String) this.values.get(name);
    }

    /**
     * Returns the named sub-pipelines map stored under {@code name}, with each value frozen
     * to a {@link ConcurrentList}.
     *
     * @param name the field name
     * @return the sub-pipelines map
     * @throws ClassCastException when the field is present but not a sub-pipelines map
     * @throws NullPointerException when the field is absent
     */
    @SuppressWarnings("unchecked")
    public @NotNull Map<String, ConcurrentList<Stage<?, ?>>> getSubPipelines(@NotNull String name) {
        Map<String, ? extends List<? extends Stage<?, ?>>> raw =
            (Map<String, ? extends List<? extends Stage<?, ?>>>) this.values.get(name);
        LinkedHashMap<String, ConcurrentList<Stage<?, ?>>> frozen = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : raw.entrySet())
            frozen.put(entry.getKey(), Concurrent.newUnmodifiableList((List<Stage<?, ?>>) entry.getValue()));
        return Map.copyOf(frozen);
    }

    /**
     * Returns the singular sub-pipeline stored under {@code name}, frozen to a
     * {@link ConcurrentList}.
     *
     * @param name the field name
     * @return the sub-pipeline
     * @throws ClassCastException when the field is present but not a list of stages
     * @throws NullPointerException when the field is absent
     */
    @SuppressWarnings("unchecked")
    public @NotNull ConcurrentList<Stage<?, ?>> getSubPipeline(@NotNull String name) {
        List<? extends Stage<?, ?>> raw = (List<? extends Stage<?, ?>>) this.values.get(name);
        return Concurrent.newUnmodifiableList((List<Stage<?, ?>>) raw);
    }

    /**
     * Returns the typed sub-pipelines map stored under {@code name}, with each chain frozen
     * to a {@link ConcurrentList}.
     *
     * @param name the field name
     * @return the typed sub-pipelines map
     * @throws ClassCastException when the field is present but not a typed sub-pipelines map
     * @throws NullPointerException when the field is absent
     */
    @SuppressWarnings("unchecked")
    public @NotNull Map<String, TypedSubPipeline> getTypedSubPipelines(@NotNull String name) {
        Map<String, TypedSubPipeline> raw = (Map<String, TypedSubPipeline>) this.values.get(name);
        LinkedHashMap<String, TypedSubPipeline> frozen = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSubPipeline> entry : raw.entrySet()) {
            TypedSubPipeline v = entry.getValue();
            frozen.put(entry.getKey(), new TypedSubPipeline(
                v.outputType(),
                Concurrent.newUnmodifiableList((List<Stage<?, ?>>) v.chain())
            ));
        }
        return Map.copyOf(frozen);
    }

    /**
     * Returns whether the configuration has a value for {@code name}.
     *
     * @param name the field name
     * @return {@code true} when the field is present
     */
    public boolean has(@NotNull String name) {
        return this.values.containsKey(name);
    }

    /**
     * Returns the raw value stored under {@code name}, or {@code null} when absent.
     *
     * @param name the field name
     * @return the raw value, or {@code null}
     */
    public @Nullable Object raw(@NotNull String name) {
        return this.values.get(name);
    }

}
