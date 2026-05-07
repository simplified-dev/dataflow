package dev.sbs.dataflow.stage;

/**
 * Coarse grouping used by UI palette renderers to bucket {@link StageKind} values into
 * pickable categories.
 * <p>
 * The categories mirror the dataflow source layout: e.g. {@code TRANSFORM_DOM} groups
 * every kind whose impl lives in {@code dev.sbs.dataflow.stage.transform.dom}.
 */
public enum StageCategory {

    /** Source stages that produce a value with no upstream input. */
    SOURCE,

    /** Stages that run a saved pipeline as a single step. */
    EMBED,

    /** Terminal stages that fan an input into named sub-pipelines. */
    BRANCH,

    /** List-reducing terminal stages (first / last / list / set / join). */
    COLLECT,

    /** String-valued list filters (contains, matches, starts-with, ends-with, equals, non-empty). */
    FILTER_STRING,

    /** List-shape filters (distinct, not-null, take, skip, index-in-range). */
    FILTER_LIST,

    /** Numeric range filters (int / long / double, greater-than / less-than / in-range). */
    FILTER_NUMERIC,

    /** DOM filters (text-contains, text-matches, has-attr, tag-equals). */
    FILTER_DOM,

    /** JSON filters (has-field, field-equals). */
    FILTER_JSON,

    /** String-valued transforms (lowercase, regex, trim, replace, length, prefix...). */
    TRANSFORM_STRING,

    /** Primitive arithmetic and parsing transforms (parse-int, abs, negate, to-string...). */
    TRANSFORM_PRIMITIVE,

    /** List-shape transforms (length, reverse). */
    TRANSFORM_LIST,

    /** Jsoup-backed DOM transforms (parse-html, css-select, node-text, nth-child...). */
    TRANSFORM_DOM,

    /** Gson-backed JSON transforms (parse-json, parse-xml, json-path, json-as-*...). */
    TRANSFORM_JSON,

    /** Encoding transforms (base64, url-encode/decode). */
    TRANSFORM_ENCODING,

}
