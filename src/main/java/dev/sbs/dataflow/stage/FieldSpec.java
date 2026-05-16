package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.stage.meta.Configurable;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.ChainSerde;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.chain.TypedChain;
import dev.sbs.dataflow.stage.meta.StageReflection;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.Function;

/**
 * Declares one slot in a stage's configuration schema.
 * <p>
 * Carries the wire name + {@link Type} discriminator + UI hints, plus typed
 * {@link #get(StageConfig)} / {@link #put(StageConfig.Builder, Object)} accessors that
 * route to the correct {@link StageConfig} reader and {@link StageConfig.Builder} writer
 * based on {@link #type}. Instances are built reflectively by {@link StageReflection} from
 * the canonical factory's {@link Configurable} parameter annotations; no hand-authored
 * call sites remain.
 *
 * @param name the field name, also the JSON property key and the UI input id
 * @param type the slot's discriminator
 * @param label human-friendly title shown next to the field's input
 * @param placeholder example value shown inside an empty input
 * @param optional whether the slot may be absent from the populated {@link StageConfig}
 * @param <T> caller-facing value type for the slot
 */
public record FieldSpec<T>(
    @NotNull String name,
    @NotNull Type type,
    @NotNull String label,
    @NotNull String placeholder,
    boolean optional
) {

    /**
     * Reads this slot's value from a populated configuration. Routes to the appropriate
     * {@link StageConfig} getter based on {@link #type}.
     *
     * @param cfg the populated configuration
     * @return the slot's value
     */
    @SuppressWarnings("unchecked")
    public T get(@NotNull StageConfig cfg) {
        return (T) switch (this.type) {
            case STRING                   -> cfg.getString(this.name);
            case INT                      -> cfg.getInt(this.name);
            case LONG                     -> cfg.getLong(this.name);
            case DOUBLE                   -> cfg.getDouble(this.name);
            case BOOLEAN                  -> cfg.getBoolean(this.name);
            case DATA_TYPE                -> cfg.getDataType(this.name);
            case SUB_PIPELINE             -> cfg.getSubPipeline(this.name);
            case SUB_PIPELINES_MAP        -> cfg.getSubPipelines(this.name);
            case TYPED_SUB_PIPELINES_MAP  -> cfg.getTypedSubPipelines(this.name);
        };
    }

    /**
     * Writes a value for this slot into a builder. Routes to the matching
     * {@link StageConfig.Builder} method based on {@link #type}.
     *
     * @param b the builder
     * @param value the value to store
     * @return {@code b} for chaining
     */
    @SuppressWarnings("unchecked")
    public @NotNull StageConfig.Builder put(@NotNull StageConfig.Builder b, @NotNull T value) {
        return switch (this.type) {
            case STRING                  -> b.string(this.name, (String) value);
            case INT                     -> b.integer(this.name, (Integer) value);
            case LONG                    -> b.longVal(this.name, (Long) value);
            case DOUBLE                  -> b.doubleVal(this.name, (Double) value);
            case BOOLEAN                 -> b.bool(this.name, (Boolean) value);
            case DATA_TYPE               -> b.dataType(this.name, (DataType<?>) value);
            case SUB_PIPELINE            -> b.subPipeline(this.name, (Chain) value);
            case SUB_PIPELINES_MAP       -> b.subPipelines(this.name, (NamedChains) value);
            case TYPED_SUB_PIPELINES_MAP -> b.typedSubPipelines(this.name, (Map<String, TypedChain>) value);
        };
    }

    /**
     * Writes a value retrieved as a raw {@link Object} (e.g. by reflective field reads),
     * deferring the unchecked cast to {@link #put(StageConfig.Builder, Object)}. Used by
     * the framework's default {@code config()} implementation.
     *
     * @param b the builder
     * @param value the value as an opaque {@link Object}
     * @return {@code b} for chaining
     */
    @SuppressWarnings("unchecked")
    public @NotNull StageConfig.Builder putRaw(@NotNull StageConfig.Builder b, @NotNull Object value) {
        return put(b, (T) value);
    }

    /**
     * Returns whether this slot has a value in the given configuration.
     *
     * @param cfg the configuration
     * @return {@code true} when the slot is populated
     */
    public boolean isPresent(@NotNull StageConfig cfg) {
        return cfg.has(this.name);
    }

    /**
     * Serialises a value for this slot to its JSON form. Routes to the matching JSON
     * primitive constructor or {@link ChainSerde} helper based on {@link #type}.
     *
     * @param value the slot value as an opaque {@link Object} (typically from {@link StageConfig#raw})
     * @param stageWriter recursive callback used by sub-pipeline types to serialise nested stages
     * @return the JSON form
     */
    @SuppressWarnings("unchecked")
    public @NotNull JsonElement writeJson(
        @NotNull Object value,
        @NotNull Function<Stage<?, ?>, JsonObject> stageWriter
    ) {
        return switch (this.type) {
            case STRING                  -> new JsonPrimitive((String) value);
            case INT                     -> new JsonPrimitive((Integer) value);
            case LONG                    -> new JsonPrimitive((Long) value);
            case DOUBLE                  -> new JsonPrimitive((Double) value);
            case BOOLEAN                 -> new JsonPrimitive((Boolean) value);
            case DATA_TYPE               -> new JsonPrimitive(((DataType<?>) value).label());
            case SUB_PIPELINE            -> ChainSerde.writeChain((Chain) value, stageWriter);
            case SUB_PIPELINES_MAP       -> ChainSerde.writeNamedChains((NamedChains) value, stageWriter);
            case TYPED_SUB_PIPELINES_MAP -> ChainSerde.writeTypedNamedChains((Map<String, TypedChain>) value, stageWriter);
        };
    }

    /**
     * Deserialises a value for this slot from its JSON form into the given builder. Routes
     * to the matching {@link StageConfig.Builder} method based on {@link #type}.
     *
     * @param raw the JSON form
     * @param b the builder to populate
     * @param stageReader recursive callback used by sub-pipeline types to deserialise nested stages
     * @return {@code b} for chaining
     * @throws IllegalArgumentException when a {@code DATA_TYPE} slot's label is not recognised by {@link DataTypes#byLabel}
     */
    public @NotNull StageConfig.Builder readJson(
        @NotNull JsonElement raw,
        @NotNull StageConfig.Builder b,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        switch (this.type) {
            case STRING    -> b.string(this.name, raw.getAsString());
            case INT       -> b.integer(this.name, raw.getAsInt());
            case LONG      -> b.longVal(this.name, raw.getAsLong());
            case DOUBLE    -> b.doubleVal(this.name, raw.getAsDouble());
            case BOOLEAN   -> b.bool(this.name, raw.getAsBoolean());
            case DATA_TYPE -> {
                String label = raw.getAsString();
                DataType<?> resolved = DataTypes.byLabel(label);
                if (resolved == null)
                    throw new IllegalArgumentException("Unknown DataType label: '" + label + "'");
                b.dataType(this.name, resolved);
            }
            case SUB_PIPELINE            -> b.subPipeline(this.name, ChainSerde.readChain(raw.getAsJsonArray(), stageReader));
            case SUB_PIPELINES_MAP       -> b.subPipelines(this.name, ChainSerde.readNamedChains(raw.getAsJsonObject(), stageReader));
            case TYPED_SUB_PIPELINES_MAP -> b.typedSubPipelines(this.name, ChainSerde.readTypedNamedChains(raw.getAsJsonObject(), stageReader));
        }
        return b;
    }

    /**
     * Discriminator for one configuration slot. Used by serde and UI code to handle each
     * slot uniformly without switching on the concrete {@link Stage} class.
     */
    public enum Type {

        /**
         * UTF-8 string.
         */
        STRING,

        /**
         * 32-bit signed integer.
         */
        INT,

        /**
         * 64-bit signed integer.
         */
        LONG,

        /**
         * 64-bit IEEE-754 floating point.
         */
        DOUBLE,

        /**
         * Boolean value.
         */
        BOOLEAN,

        /**
         * {@link DataType} reference, serialised as its label.
         */
        DATA_TYPE,

        /**
         * Map of named sub-pipelines, keyed by output name. Each value is an ordered list of
         * {@link Stage} instances forming the named output's sub-chain.
         */
        SUB_PIPELINES_MAP,

        /**
         * Single sub-pipeline, an ordered list of {@link Stage} instances. Carried by stages
         * such as map / flatMap / takeWhile that run one inner chain per element.
         */
        SUB_PIPELINE,

        /**
         * Map of named sub-pipelines that each declare an explicit output {@link DataType}.
         * Storage value is {@code Map<String, TypedChain>}. Used by stages that build a
         * structured output where each named slot has its own static type, such as the JSON
         * object builder.
         */
        TYPED_SUB_PIPELINES_MAP,

    }

}
