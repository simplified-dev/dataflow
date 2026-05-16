package dev.simplified.dataflow.chain;

import dev.simplified.dataflow.DataType;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link Chain} decorated with the {@link DataType} its last stage must produce. Used by
 * stages that build a structured value where each named slot has its own statically-known
 * output type, e.g. the JSON object builder.
 *
 * @param outputType the declared output type of the body's last stage
 * @param chain the body chain
 * @param <O> declared output type of the body
 */
public record TypedChain<O>(@NotNull DataType<O> outputType, @NotNull Chain<?, O> chain) {}
