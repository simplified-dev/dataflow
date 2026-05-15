package dev.sbs.dataflow.chain;

import dev.sbs.dataflow.DataType;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link Chain} decorated with the {@link DataType} its last stage must produce. Used by
 * stages that build a structured value where each named slot has its own statically-known
 * output type, e.g. the JSON object builder.
 *
 * @param outputType the declared output type of the body's last stage
 * @param chain the body chain
 */
public record TypedChain(@NotNull DataType<?> outputType, @NotNull Chain chain) {}
