package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.chain.Chain;
import dev.simplified.reflection.accessor.MethodAccessor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Reflection-derived metadata for one {@link Stage} implementation class.
 * <p>
 * Built once per class by {@link StageReflection#of(Class)} and cached. Carries the
 * class-level {@link StageSpec} annotation, the ordered list of configurable {@link SlotInfo
 * slots} derived from the factory method's {@link Configurable} parameters, and an
 * accessor for the canonical factory.
 * <p>
 * Each {@link SlotInfo} bundles the wire-side {@link FieldSpec}, the matching Java
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
    @NotNull List<SlotInfo<?>> slots,
    @NotNull MethodAccessor<?> factory
) {

    /**
     * Returns just the wire-side {@link FieldSpec}s in factory parameter order. Convenience
     * for JSON writers and UI schema renderers.
     *
     * @return the wire-side schema, in factory parameter order
     */
    public @NotNull List<FieldSpec<?>> schema() {
        List<FieldSpec<?>> specs = new ArrayList<>(this.slots.size());
        for (SlotInfo<?> slot : this.slots) specs.add(slot.spec());
        return List.copyOf(specs);
    }

    /**
     * Builds a {@link StageConfig} from a {@link Stage} instance by reading each
     * configured field's current value via its {@link SlotInfo#instanceField} and writing
     * it to the matching {@link FieldSpec wire slot}.
     *
     * @param stage the instance whose config to serialise
     * @return the populated configuration
     */
    public @NotNull StageConfig buildConfig(@NotNull Stage<?, ?> stage) {
        StageConfig.Builder builder = StageConfig.builder();
        for (SlotInfo<?> slot : this.slots) {
            Object value = slot.instanceField().get(stage);
            if (value == null) continue;
            slot.spec().putRaw(builder, value);
        }
        return builder.build();
    }

    /**
     * Reconstructs a {@link Stage} by reading each slot from {@code cfg} through its
     * {@link FieldSpec}, applying the slot's {@link SlotInfo#argAdapter}, and invoking the
     * canonical factory with the resulting arguments.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public @NotNull Stage<?, ?> fromConfig(@NotNull StageConfig cfg) {
        Object[] args = new Object[this.slots.size()];
        for (int i = 0; i < this.slots.size(); i++) {
            SlotInfo<?> slot = this.slots.get(i);
            FieldSpec<?> spec = slot.spec();
            Object raw = spec.optional() && !spec.isPresent(cfg) ? null : spec.get(cfg);
            args[i] = raw == null ? null : slot.argAdapter().apply(raw);
        }
        return (Stage<?, ?>) this.factory.invoke(null, args);
    }

}
