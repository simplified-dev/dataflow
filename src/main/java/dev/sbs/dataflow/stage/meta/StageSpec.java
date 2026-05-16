package dev.sbs.dataflow.stage.meta;

import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.FieldSpec;
import dev.sbs.dataflow.stage.StageRegistry;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Class-level metadata for a {@link Stage} implementation.
 * <p>
 * Carries everything the framework needs about a stage that is not derivable from its
 * factory signature: a stable wire-format {@link #id() id}, a display name, a
 * type-signature description, and a coarse {@link Category} grouping. The framework reads
 * this annotation reflectively (cached per-class on first touch) so authors never
 * hand-author {@link FieldSpec} lists or registry entries.
 * <p>
 * The factory is discovered separately: a stage author declares a single
 * {@code public static of(...)} method whose parameters carry {@link Configurable}, and
 * the framework derives schema, {@code config()}, and the deserialisation factory from
 * that signature.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StageSpec {

    /**
     * Stable wire-format discriminator for this stage. Persisted in JSON as the
     * {@code "kind"} field and looked up by {@link StageRegistry#byId(String)}. Renaming
     * this value breaks any already-stored pipeline JSON.
     *
     * @return the wire-format id
     */
    @NotNull String id();

    /**
     * Human-friendly display name shown in UI palettes.
     *
     * @return the display name
     */
    @NotNull String displayName();

    /**
     * Short type-signature description (e.g. {@code "List<T> -> INT"}).
     *
     * @return the description
     */
    @NotNull String description();

    /**
     * Coarse category used by UI palettes and docs to bucket stages.
     *
     * @return the category
     */
    @NotNull Category category();

    /**
     * Coarse grouping used by UI palette renderers to bucket stages into pickable categories.
     * <p>
     * The categories mirror the dataflow source layout: e.g. {@code TRANSFORM_DOM} groups
     * every kind whose impl lives in {@code dev.sbs.dataflow.stage.transform.dom}. Declaration
     * order is meaningful: {@link #SOURCE} comes first, the {@code TERMINAL_*} block comes last,
     * and everything in between is sorted alphabetically.
     * <p>
     * Downstream UI code can rely on {@link Enum#ordinal()} for natural display order.
     */
    enum Category {

        /**
         * Source stages that produce a value with no upstream input.
         */
        SOURCE,

        /**
         * DOM filters (text-contains, text-matches, has-attr, tag-equals).
         */
        FILTER_DOM,

        /**
         * JSON filters (has-field, field-equals).
         */
        FILTER_JSON,

        /**
         * List-shape filters (distinct, not-null, take, skip, index-in-range).
         */
        FILTER_LIST,

        /**
         * Numeric range filters (int / long / double, greater-than / less-than / in-range).
         */
        FILTER_NUMERIC,

        /**
         * String-valued list filters (contains, matches, starts-with, ends-with, equals, non-empty).
         */
        FILTER_STRING,

        /**
         * Type-agnostic single-element predicates (not-null, not, and, or).
         */
        PREDICATE_COMMON,

        /**
         * DOM single-element predicates (text-contains, text-matches, has-attr, tag-equals).
         */
        PREDICATE_DOM,

        /**
         * JSON single-element predicates (has-field, field-equals).
         */
        PREDICATE_JSON,

        /**
         * Numeric single-element predicates (greater-than, less-than, in-range for int / long / double).
         */
        PREDICATE_NUMERIC,

        /**
         * String-valued single-element predicates (contains, starts/ends-with, equals, matches, non-empty).
         */
        PREDICATE_STRING,

        /**
         * Jsoup-backed DOM transforms (parse-html, css-select, node-text, nth-child...).
         */
        TRANSFORM_DOM,

        /**
         * Encoding transforms (base64, url-encode/decode).
         */
        TRANSFORM_ENCODING,

        /**
         * Gson-backed JSON transforms (parse-json, parse-xml, json-path, json-as-*...).
         */
        TRANSFORM_JSON,

        /**
         * List-shape transforms (length, reverse).
         */
        TRANSFORM_LIST,

        /**
         * Primitive arithmetic and parsing transforms (parse-int, abs, negate, to-string...).
         */
        TRANSFORM_PRIMITIVE,

        /**
         * String-valued transforms (lowercase, regex, trim, replace, length, prefix...).
         */
        TRANSFORM_STRING,

        /**
         * Arithmetic-mean terminals (average-int / average-long / average-double).
         */
        TERMINAL_AVERAGE,

        /**
         * Structural-reduction terminals (first / last / list / set / join).
         */
        TERMINAL_COLLECT,

        /**
         * Predicate-based short-circuit terminals (any-match, all-match, none-match, find-first).
         */
        TERMINAL_MATCH,

        /**
         * Ordering-based selection terminals (min, max, min-by, max-by).
         */
        TERMINAL_MINMAX,

        /**
         * Numeric-summation terminals (count, sum-int / sum-long / sum-double).
         */
        TERMINAL_SUM,

    }

}
