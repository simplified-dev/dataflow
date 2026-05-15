package dev.sbs.dataflow.chain;

import dev.sbs.dataflow.stage.Stage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Mutable builder for {@link Chain}. Stages are appended in order; the resulting chain is
 * frozen at {@link #build()}.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class ChainBuilder {

    private final @NotNull ConcurrentList<Stage<?, ?>> stages = Concurrent.newList();

    /**
     * Appends a stage to the chain under construction.
     *
     * @param stage the stage to append
     * @return this builder
     */
    public @NotNull ChainBuilder stage(@NotNull Stage<?, ?> stage) {
        this.stages.add(stage);
        return this;
    }

    /**
     * Builds an immutable {@link Chain} from the staged stages.
     *
     * @return the built chain
     */
    public @NotNull Chain build() {
        return new Chain(Concurrent.newUnmodifiableList(this.stages));
    }

    /**
     * Exposes the in-progress stage list to package peers (e.g. for in-line validation
     * before {@link #build()} is called). External callers should use {@link #build()}.
     *
     * @return the live stage list
     */
    @NotNull ConcurrentList<Stage<?, ?>> stages() {
        return this.stages;
    }

}
