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

    /** Terminal stages that fan an input into named sub-pipelines. */
    BRANCH,

    /** Structural-reduction terminals (first / last / list / set / join). */
    TERMINAL_COLLECT,

    /** Numeric-summation terminals (count, sum-int / sum-long / sum-double). */
    TERMINAL_SUM,

    /** Arithmetic-mean terminals (average-int / average-long / average-double). */
    TERMINAL_AVERAGE,

    /** Ordering-based selection terminals (min, max, min-by, max-by). */
    TERMINAL_MINMAX,

    /** Predicate-based short-circuit terminals (any-match, all-match, none-match, find-first). */
    TERMINAL_MATCH,

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

    /** String-valued single-element predicates (contains, starts/ends-with, equals, matches, non-empty). */
    PREDICATE_STRING,

    /** Numeric single-element predicates (greater-than, less-than, in-range for int / long / double). */
    PREDICATE_NUMERIC,

    /** DOM single-element predicates (text-contains, text-matches, has-attr, tag-equals). */
    PREDICATE_DOM,

    /** JSON single-element predicates (has-field, field-equals). */
    PREDICATE_JSON,

    /** Type-agnostic single-element predicates (not-null, not, and, or). */
    PREDICATE_COMMON,

}
