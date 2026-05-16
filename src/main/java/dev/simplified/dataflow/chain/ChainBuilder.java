package dev.simplified.dataflow.chain;

import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.stage.Stage;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Mutable builder for {@link Chain}. Phantom-typed mirror of {@link DataPipeline.Builder}:
 * seeded with an input {@link DataType}, each {@code stage} call advances the running
 * output type, and {@link #build()} freezes the assembled stages into an immutable chain.
 *
 * @param <I> input type the seeded first stage must consume
 * @param <O> running output type of the staged chain
 */
public final class ChainBuilder<I, O> {

    private final @NotNull List<Stage<?, ?>> stages = new ArrayList<>();
    private final @NotNull DataType<I> seedType;

    ChainBuilder(@NotNull DataType<I> seedType) {
        this.seedType = seedType;
    }

    /**
     * Appends a stage to the chain under construction, advancing the running output type to
     * {@code U}.
     *
     * @param stage the stage to append, whose input must be assignable from {@code O} and
     *              whose output is {@code U}
     * @return this builder, retyped to run on {@code U}
     * @param <U> the appended stage's output type
     */
    @SuppressWarnings("unchecked")
    public <U> @NotNull ChainBuilder<I, U> stage(@NotNull Stage<? super O, ? extends U> stage) {
        this.stages.add(stage);
        return (ChainBuilder<I, U>) this;
    }

    /**
     * Builds an immutable {@link Chain} from the staged stages.
     *
     * @return the built chain
     */
    public @NotNull Chain<I, O> build() {
        return new Chain<>(Concurrent.newUnmodifiableList(this.stages));
    }

    /**
     * Exposes the seed input type to package peers.
     *
     * @return the seed input type
     */
    @NotNull DataType<I> seedType() {
        return this.seedType;
    }

    /**
     * Exposes the in-progress stage list to package peers (e.g. for in-line validation
     * before {@link #build()} is called). External callers should use {@link #build()}.
     *
     * @return the live stage list
     */
    @NotNull List<Stage<?, ?>> stages() {
        return this.stages;
    }

}
