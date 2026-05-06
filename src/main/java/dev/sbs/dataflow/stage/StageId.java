package dev.sbs.dataflow.stage;

/**
 * Stable wire-format discriminator for a {@link Stage} kind. Used as the {@code "kind"}
 * field in the serialised pipeline definition so renames in the Java code do not break
 * stored pipelines.
 */
public enum StageId {

    /* ---- Source ---- */
    SOURCE_URL,
    SOURCE_PASTE,
    PARSE_HTML,
    PARSE_XML,
    PARSE_JSON,

    /* ---- Filter ---- */
    FILTER_PREDICATE,
    FILTER_DISTINCT,

    /* ---- Transform ---- */
    TRANSFORM_CSS_SELECT,
    TRANSFORM_NODE_TEXT,
    TRANSFORM_NODE_ATTR,
    TRANSFORM_NTH_CHILD,
    TRANSFORM_JSON_PATH,
    TRANSFORM_JSON_FIELD,
    TRANSFORM_REGEX_EXTRACT,
    TRANSFORM_PARSE_INT,
    TRANSFORM_PARSE_DOUBLE,
    TRANSFORM_TRIM,
    TRANSFORM_REPLACE,
    TRANSFORM_SPLIT,
    TRANSFORM_MAP,

    /* ---- Collect ---- */
    COLLECT_FIRST,
    COLLECT_LIST,
    COLLECT_SET,
    COLLECT_JOIN,

    /* ---- Compound ---- */
    BRANCH,
    PIPELINE_EMBED,

}
