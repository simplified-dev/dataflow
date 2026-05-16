package dev.simplified.dataflow.chain;

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
 * sub-pipelines (e.g. {@code MapCollect}, {@code AndPredicate}, {@code OrPredicate}).
 *
 * @param chains the named-body map; iteration order is preserved
 * @param <I> shared input type for every named chain
 */
public record NamedChains<I>(@NotNull Map<String, Chain<I, ?>> chains) {

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
        return new NamedChains<>(Map.copyOf(frozen));
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
