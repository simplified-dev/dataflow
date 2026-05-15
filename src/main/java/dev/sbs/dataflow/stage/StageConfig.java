package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.chain.TypedChain;
import dev.simplified.collection.Concurrent;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
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

        /**
         * Stores a string under {@code name} only when {@code value} is non-null. Used by
         * fields whose absence is semantically distinct from an empty string, so serde
         * round-trips do not collapse {@code null} and {@code ""} together.
         *
         * @param name the field name
         * @param value the string to store, or {@code null} to leave the field unset
         * @return this builder
         */
        public @NotNull Builder optionalString(@NotNull String name, @Nullable String value) {
            if (value != null) this.values.put(name, value);
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

        /**
         * Stores a {@link NamedChains} under {@code name}. Used by stages whose
         * configuration carries several named sub-pipeline bodies (e.g. MapCollect,
         * And/OrPredicate).
         *
         * @param name the field name
         * @param value the named-bodies map
         * @return this builder
         */
        public @NotNull Builder subPipelines(@NotNull String name, @NotNull NamedChains value) {
            this.values.put(name, value);
            return this;
        }

        /**
         * Stores a {@link Chain} under {@code name}. Used by stages such as Map / FlatMap /
         * TakeWhile that carry exactly one inner chain.
         *
         * @param name the field name
         * @param chain the inner chain
         * @return this builder
         */
        public @NotNull Builder subPipeline(@NotNull String name, @NotNull Chain chain) {
            this.values.put(name, chain);
            return this;
        }

        /**
         * Stores a typed named-chains map under {@code name}.
         *
         * @param name the field name
         * @param value the typed sub-pipelines map
         * @return this builder
         */
        public @NotNull Builder typedSubPipelines(@NotNull String name, @NotNull Map<String, TypedChain> value) {
            this.values.put(name, value);
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
     * Returns the {@link NamedChains} stored under {@code name}.
     *
     * @param name the field name
     * @return the named-bodies map
     * @throws ClassCastException when the field is present but not a {@link NamedChains}
     * @throws NullPointerException when the field is absent
     */
    public @NotNull NamedChains getSubPipelines(@NotNull String name) {
        return (NamedChains) this.values.get(name);
    }

    /**
     * Returns the {@link Chain} stored under {@code name}.
     *
     * @param name the field name
     * @return the chain
     * @throws ClassCastException when the field is present but not a {@link Chain}
     * @throws NullPointerException when the field is absent
     */
    public @NotNull Chain getSubPipeline(@NotNull String name) {
        return (Chain) this.values.get(name);
    }

    /**
     * Returns the typed named-chains map stored under {@code name}.
     *
     * @param name the field name
     * @return the typed sub-pipelines map
     * @throws ClassCastException when the field is present but not a typed sub-pipelines map
     * @throws NullPointerException when the field is absent
     */
    @SuppressWarnings("unchecked")
    public @NotNull Map<String, TypedChain> getTypedSubPipelines(@NotNull String name) {
        return (Map<String, TypedChain>) this.values.get(name);
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
