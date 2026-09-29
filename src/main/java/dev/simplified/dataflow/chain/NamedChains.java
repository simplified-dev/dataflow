package dev.simplified.dataflow.chain;

import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.stage.Stage;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable map of named {@link Chain} bodies that share the same input type {@code I} but
 * may produce heterogeneous outputs.
 * <p>
 * Carried by stages whose configuration fans the same input value through several named
 * sub-pipelines (e.g. {@code MapCollect}, {@code AndPredicate}, {@code OrPredicate}). The map
 * is held as an unmodifiable copy in the iteration order it was given, so the declared order
 * of the bodies survives into execution and back onto the wire.
 *
 * @param chains the named-body map, in declared order
 * @param <I> shared input type for every named chain
 */
public record NamedChains<I>(@NotNull Map<String, Chain<I, ?>> chains) {

    /**
     * Copies {@code chains} into an unmodifiable map that keeps its iteration order.
     *
     * @param chains the named-body map, in declared order
     */
    public NamedChains {
        chains = Concurrent.newUnmodifiableLinkedMap(chains);
    }

    /**
     * Wraps a raw {@code Map<String, List<Stage>>} as a {@link NamedChains}, freezing each
     * entry's stages via {@link Chain#of(List)}.
     *
     * @param raw the raw named-bodies map
     * @return a frozen named-chains map
     * @param <I> shared input type for every named chain
     */
    public static <I> @NotNull NamedChains<I> of(
        @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> raw
    ) {
        LinkedHashMap<String, Chain<I, ?>> frozen = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : raw.entrySet())
            frozen.put(entry.getKey(), Chain.of(entry.getValue()));
        return new NamedChains<>(frozen);
    }

    /**
     * Number of named entries.
     *
     * @return the entry count
     */
    public int size() {
        return this.chains.size();
    }

    /**
     * Whether this map has zero entries.
     *
     * @return {@code true} when empty
     */
    public boolean isEmpty() {
        return this.chains.isEmpty();
    }

}
