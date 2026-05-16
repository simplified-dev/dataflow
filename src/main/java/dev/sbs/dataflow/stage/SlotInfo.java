package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.chain.NamedChains;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * One configurable slot's reflection-derived metadata, bundling the wire-side
 * {@link FieldSpec}, the matching Java parameter name (which is also the instance field
 * name), a {@link FieldAccessor} for reading the live instance value, and an adapter that
 * converts the cfg-side value into the factory parameter value.
 * <p>
 * The Java parameter name and the wire key are tracked separately because a few stages
 * use {@code @Configurable(name = ...)} to override the wire key while the matching
 * instance field still uses the Java identifier (e.g. {@code MapTransform}'s
 * {@code inputElementType} field / parameter against the {@code elementInputType} wire
 * key).
 * <p>
 * The {@link #argAdapter} bridges sub-pipeline factory parameter conventions: the wire
 * format always stores {@link Chain} / {@link NamedChains},
 * but several factories accept the looser {@code List<? extends Stage<?, ?>>} /
 * {@code Map<String, List<? extends Stage<?, ?>>>} for ergonomics. The adapter is the
 * identity function for matching shapes.
 *
 * @param spec the wire-side schema entry (key, type, label, placeholder, optional)
 * @param paramName the Java parameter name on the factory, also the instance field name
 * @param instanceField accessor for the matching instance field
 * @param argAdapter adapts a {@link StageConfig}-side value to the factory parameter value
 * @param <T> caller-facing wire type for this slot
 */
public record SlotInfo<T>(
    @NotNull FieldSpec<T> spec,
    @NotNull String paramName,
    @NotNull FieldAccessor<?> instanceField,
    @NotNull Function<Object, Object> argAdapter
) {

    /**
     * Identity adapter, returned for slots whose wire type matches the factory parameter
     * type one-to-one.
     */
    public static final @NotNull Function<Object, Object> IDENTITY = Function.identity();

}
