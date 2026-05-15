package dev.sbs.dataflow.chain;

import dev.sbs.dataflow.stage.Stage;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable map of named {@link Chain} bodies, keyed by output name in insertion order.
 * <p>
 * Carried by stages whose configuration fans the same input value through several named
 * sub-pipelines (e.g. {@code MapCollect}, {@code AndPredicate}, {@code OrPredicate}).
 *
 * @param chains the named-body map; iteration order is preserved
 */
public record NamedChains(@NotNull Map<String, Chain> chains) {

    /**
     * Wraps a raw {@code Map<String, List<Stage>>} as a {@link NamedChains}, freezing each
     * entry's stages via {@link Chain#of(List)}.
     *
     * @param raw the raw named-bodies map
     * @return a frozen named-chains map
     */
    public static @NotNull NamedChains of(
        @NotNull Map<String, ? extends List<? extends Stage<?, ?>>> raw
    ) {
        LinkedHashMap<String, Chain> frozen = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends List<? extends Stage<?, ?>>> entry : raw.entrySet())
            frozen.put(entry.getKey(), Chain.of(entry.getValue()));
        return new NamedChains(Map.copyOf(frozen));
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
