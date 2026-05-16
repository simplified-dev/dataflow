package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.sbs.dataflow.chain.TypedChain;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Declares one slot in a {@link StageKind}'s configuration schema.
 * <p>
 * Carries the wire name + {@link FieldType} discriminator + UI hints, plus typed
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
    @NotNull FieldType type,
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

}
