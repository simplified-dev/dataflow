package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;

/**
 * Discriminator for one configuration slot in a {@link FieldSpec}. Used by serde and UI
 * code to handle each slot uniformly without switching on the concrete {@link Stage} class.
 */
public enum FieldType {

    /**
     * UTF-8 string.
     */
    STRING,

    /**
     * 32-bit signed integer.
     */
    INT,

    /**
     * 64-bit signed integer.
     */
    LONG,

    /**
     * 64-bit IEEE-754 floating point.
     */
    DOUBLE,

    /**
     * Boolean value.
     */
    BOOLEAN,

    /**
     * {@link DataType} reference, serialised as its label.
     */
    DATA_TYPE,

    /**
     * Map of named sub-pipelines, keyed by output name. Each value is an ordered list of
     * {@link Stage} instances forming the named output's sub-chain.
     */
    SUB_PIPELINES_MAP,

    /**
     * Single sub-pipeline, an ordered list of {@link Stage} instances. Carried by stages
     * such as map / flatMap / takeWhile that run one inner chain per element.
     */
    SUB_PIPELINE,

    /**
     * Map of named sub-pipelines that each declare an explicit output {@link DataType}.
     * Storage value is {@code Map<String, TypedChain>}. Used by stages that build a
     * structured output where each named slot has its own static type, such as the JSON
     * object builder.
     */
    TYPED_SUB_PIPELINES_MAP,

}
