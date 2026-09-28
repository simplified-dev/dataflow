package dev.simplified.dataflow.stage;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.chain.ChainSerde;
import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.chain.TypedChain;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageReflection;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
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
            case PIPELINE                 -> cfg.getPipeline(this.name);
            case STRING_MAP               -> cfg.getStringMap(this.name);
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
            case SUB_PIPELINE            -> b.subPipeline(this.name, (Chain<?, ?>) value);
            case SUB_PIPELINES_MAP       -> b.subPipelines(this.name, (NamedChains<?>) value);
            case TYPED_SUB_PIPELINES_MAP -> b.typedSubPipelines(this.name, (Map<String, TypedChain<?>>) value);
            case PIPELINE                -> b.pipeline(this.name, (DataPipeline<?>) value);
            case STRING_MAP              -> b.stringMap(this.name, (Map<String, String>) value);
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
            case SUB_PIPELINE            -> ChainSerde.writeChain((Chain<?, ?>) value, stageWriter);
            case SUB_PIPELINES_MAP       -> ChainSerde.writeNamedChains((NamedChains<?>) value, stageWriter);
            case TYPED_SUB_PIPELINES_MAP -> ChainSerde.writeTypedNamedChains((Map<String, TypedChain<?>>) value, stageWriter);
            case PIPELINE                -> ChainSerde.writePipeline((DataPipeline<?>) value, stageWriter);
            case STRING_MAP              -> writeStringMap((Map<String, String>) value);
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
     * @throws IllegalArgumentException when an {@code INT} or {@code LONG} slot's number is not integral or does not fit the type
     * @throws IllegalArgumentException when a {@code DATA_TYPE} slot's label is not recognised by {@link DataTypes#byLabel}
     * @throws IllegalArgumentException when a {@code STRING_MAP} slot maps a key to a JSON null, object or array
     * @throws IllegalStateException when a {@code PIPELINE} slot's stage array does not form a valid pipeline
     */
    public @NotNull StageConfig.Builder readJson(
        @NotNull JsonElement raw,
        @NotNull StageConfig.Builder b,
        @NotNull Function<JsonObject, Stage<?, ?>> stageReader
    ) {
        switch (this.type) {
            case STRING    -> b.string(this.name, raw.getAsString());
            case INT       -> b.integer(this.name, (int) this.readIntegral(raw));
            case LONG      -> b.longVal(this.name, this.readIntegral(raw));
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
            case PIPELINE                -> b.pipeline(this.name, ChainSerde.readPipeline(raw.getAsJsonArray(), stageReader));
            case STRING_MAP              -> b.stringMap(this.name, this.readStringMap(raw.getAsJsonObject()));
        }
        return b;
    }

    /**
     * Reads an {@code INT} or {@code LONG} slot's number exactly, so a fraction or a value past
     * the slot's range is refused rather than truncated or wrapped.
     *
     * @param raw the JSON form, a number or a numeric string
     * @return the value, within the {@code int} range for an {@code INT} slot
     * @throws IllegalArgumentException when the number is not integral or does not fit the slot's type
     */
    private long readIntegral(@NotNull JsonElement raw) {
        BigDecimal value = raw.getAsBigDecimal();

        try {
            return this.type == Type.INT ? value.intValueExact() : value.longValueExact();
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException(
                "Field '" + this.name + "' holds '" + raw + "' but an integral " + this.type + " was expected", ex
            );
        }
    }

    private static @NotNull JsonObject writeStringMap(@NotNull Map<String, String> value) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, String> entry : value.entrySet())
            out.addProperty(entry.getKey(), entry.getValue());
        return out;
    }

    private @NotNull Map<String, String> readStringMap(@NotNull JsonObject raw) {
        Map<String, String> out = new LinkedHashMap<>();

        for (Map.Entry<String, JsonElement> entry : raw.entrySet()) {
            JsonElement value = entry.getValue();

            if (!value.isJsonPrimitive())
                throw new IllegalArgumentException(
                    "Field '" + this.name + "' maps '" + entry.getKey() + "' to '" + value + "' but a string was expected"
                );

            out.put(entry.getKey(), value.getAsString());
        }

        return out;
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

        /**
         * Whole sourced pipeline carried as an operand, storage value {@link DataPipeline}. Its
         * stage 0 is a source, so the stage that owns it reads a second document beside its
         * input. Serialised as a stage array in the shape of a pipeline file and validated as one
         * when read; the owning stage evaluates it through
         * {@link PipelineContext#evaluateOperand(DataPipeline)}.
         */
        PIPELINE,

        /**
         * Map of string keys to string values, storage value {@code Map<String, String>} that
         * keeps insertion order. Serialised as a JSON object whose values are strings.
         */
        STRING_MAP,

    }

}
