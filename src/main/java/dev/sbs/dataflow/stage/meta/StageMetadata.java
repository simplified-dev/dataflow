package dev.sbs.dataflow.stage.meta;

import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.StageConfig;

import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.reflection.accessor.MethodAccessor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Reflection-derived metadata for one {@link Stage} implementation class.
 * <p>
 * Built once per class by {@link StageReflection#of(Class)} and cached. Carries the
 * class-level {@link StageSpec} annotation, the ordered list of configurable {@link Slot
 * slots} derived from the factory method's {@link Configurable} parameters, and an
 * accessor for the canonical factory.
 * <p>
 * Each {@link Slot} bundles the wire-side {@link FieldSpec}, the matching Java
 * parameter / instance-field name, the field accessor used by {@link #buildConfig}, and
 * the adapter used by {@link #fromConfig} to bridge factory parameter types (e.g.
 * {@code List<? extends Stage<?, ?>>}) against the wire-side
 * {@link Chain}.
 *
 * @param annotation the class-level {@link StageSpec} annotation
 * @param slots the ordered configurable slots, derived from factory parameters
 * @param factory accessor for the canonical {@code public static of(...)} method
 */
public record StageMetadata(
    @NotNull StageSpec annotation,
    @NotNull List<Slot<?>> slots,
    @NotNull MethodAccessor<?> factory
) {

    /**
     * One configurable slot's reflection-derived metadata, bundling the wire-side
     * {@link FieldSpec}, the matching Java parameter name (which is also the instance field
     * name), a {@link FieldAccessor} for reading the live instance value, and an adapter
     * that converts the cfg-side value into the factory parameter value.
     * <p>
     * The Java parameter name and the wire key are tracked separately because a few stages
     * use {@code @Configurable(name = ...)} to override the wire key while the matching
     * instance field still uses the Java identifier.
     * <p>
     * The {@link #argAdapter} bridges sub-pipeline factory parameter conventions: the wire
     * format always stores {@link Chain} / {@link NamedChains}, but several factories
     * accept the looser {@code List<? extends Stage<?, ?>>} /
     * {@code Map<String, List<? extends Stage<?, ?>>>} for ergonomics. The adapter is the
     * identity function for matching shapes.
     *
     * @param spec the wire-side schema entry (key, type, label, placeholder, optional)
     * @param paramName the Java parameter name on the factory, also the instance field name
     * @param instanceField accessor for the matching instance field
     * @param argAdapter adapts a {@link StageConfig}-side value to the factory parameter value
     * @param <T> caller-facing wire type for this slot
     */
    public record Slot<T>(
        @NotNull FieldSpec<T> spec,
        @NotNull String paramName,
        @NotNull FieldAccessor<?> instanceField,
        @NotNull Function<Object, Object> argAdapter
    ) {

        /**
         * Identity adapter, returned for slots whose wire type matches the factory
         * parameter type one-to-one.
         */
        public static final @NotNull Function<Object, Object> IDENTITY = Function.identity();

    }

    /**
     * Returns just the wire-side {@link FieldSpec}s in factory parameter order. Convenience
     * for JSON writers and UI schema renderers.
     *
     * @return the wire-side schema, in factory parameter order
     */
    public @NotNull List<FieldSpec<?>> schema() {
        List<FieldSpec<?>> specs = new ArrayList<>(this.slots.size());
        for (Slot<?> slot : this.slots) specs.add(slot.spec());
        return List.copyOf(specs);
    }

    /**
     * Builds a {@link StageConfig} from a {@link Stage} instance by reading each
     * configured field's current value via its {@link Slot#instanceField} and writing
     * it to the matching {@link FieldSpec wire slot}.
     *
     * @param stage the instance whose config to serialise
     * @return the populated configuration
     */
    public @NotNull StageConfig buildConfig(@NotNull Stage<?, ?> stage) {
        StageConfig.Builder builder = StageConfig.builder();
        for (Slot<?> slot : this.slots) {
            Object value = slot.instanceField().get(stage);
            if (value == null) continue;
            slot.spec().putRaw(builder, value);
        }
        return builder.build();
    }

    /**
     * Reconstructs a {@link Stage} by reading each slot from {@code cfg} through its
     * {@link FieldSpec}, applying the slot's {@link Slot#argAdapter}, and invoking the
     * canonical factory with the resulting arguments.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public @NotNull Stage<?, ?> fromConfig(@NotNull StageConfig cfg) {
        Object[] args = new Object[this.slots.size()];
        for (int i = 0; i < this.slots.size(); i++) {
            Slot<?> slot = this.slots.get(i);
            FieldSpec<?> spec = slot.spec();
            Object raw = spec.optional() && !spec.isPresent(cfg) ? null : spec.get(cfg);
            args[i] = raw == null ? null : slot.argAdapter().apply(raw);
        }
        return (Stage<?, ?>) this.factory.invoke(null, args);
    }

}
