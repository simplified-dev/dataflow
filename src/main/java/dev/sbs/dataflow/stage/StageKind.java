package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.stage.branch.Branch;
import dev.sbs.dataflow.stage.collect.FirstCollect;
import dev.sbs.dataflow.stage.collect.JoinCollect;
import dev.sbs.dataflow.stage.collect.LastCollect;
import dev.sbs.dataflow.stage.collect.ListCollect;
import dev.sbs.dataflow.stage.collect.SetCollect;
import dev.sbs.dataflow.stage.embed.PipelineEmbed;
import dev.sbs.dataflow.stage.filter.dom.DomHasAttrFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTagEqualsFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextContainsFilter;
import dev.sbs.dataflow.stage.filter.dom.DomTextMatchesFilter;
import dev.sbs.dataflow.stage.filter.json.JsonFieldEqualsFilter;
import dev.sbs.dataflow.stage.filter.json.JsonHasFieldFilter;
import dev.sbs.dataflow.stage.filter.list.DistinctFilter;
import dev.sbs.dataflow.stage.filter.list.IndexInRangeFilter;
import dev.sbs.dataflow.stage.filter.list.NotNullFilter;
import dev.sbs.dataflow.stage.filter.list.SkipFilter;
import dev.sbs.dataflow.stage.filter.list.TakeFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.DoubleLessThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.IntLessThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongGreaterThanFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongInRangeFilter;
import dev.sbs.dataflow.stage.filter.numeric.LongLessThanFilter;
import dev.sbs.dataflow.stage.filter.string.StringContainsFilter;
import dev.sbs.dataflow.stage.filter.string.StringEndsWithFilter;
import dev.sbs.dataflow.stage.filter.string.StringEqualsFilter;
import dev.sbs.dataflow.stage.filter.string.StringMatchesFilter;
import dev.sbs.dataflow.stage.filter.string.StringNonEmptyFilter;
import dev.sbs.dataflow.stage.filter.string.StringStartsWithFilter;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.source.UrlSource;
import dev.sbs.dataflow.stage.transform.dom.CssSelectTransform;
import dev.sbs.dataflow.stage.transform.dom.DomChildrenTransform;
import dev.sbs.dataflow.stage.transform.dom.DomOuterHtmlTransform;
import dev.sbs.dataflow.stage.transform.dom.DomOwnTextTransform;
import dev.sbs.dataflow.stage.transform.dom.DomParentTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeAttrTransform;
import dev.sbs.dataflow.stage.transform.dom.NodeTextTransform;
import dev.sbs.dataflow.stage.transform.dom.NthChildTransform;
import dev.sbs.dataflow.stage.transform.dom.ParseHtmlTransform;
import dev.sbs.dataflow.stage.transform.encoding.Base64DecodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.Base64EncodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.UrlDecodeTransform;
import dev.sbs.dataflow.stage.transform.encoding.UrlEncodeTransform;
import dev.sbs.dataflow.stage.transform.json.*;
import dev.sbs.dataflow.stage.transform.list.ListLengthTransform;
import dev.sbs.dataflow.stage.transform.list.ReverseTransform;
import dev.sbs.dataflow.stage.transform.primitive.*;
import dev.sbs.dataflow.stage.transform.string.LowerCaseTransform;
import dev.sbs.dataflow.stage.transform.string.PrefixTransform;
import dev.sbs.dataflow.stage.transform.string.RegexExtractTransform;
import dev.sbs.dataflow.stage.transform.string.ReplaceTransform;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import dev.sbs.dataflow.stage.transform.string.SuffixTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import dev.sbs.dataflow.stage.transform.string.UpperCaseTransform;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Stable wire-format discriminator for a {@link Stage} kind.
 * <p>
 * Each enum constant carries everything the framework needs about a stage: display name,
 * type-signature description, coarse {@link StageCategory} grouping, configuration
 * {@link FieldSpec schema}, and the {@link Function factory} that builds a fresh stage
 * instance from a populated {@link StageConfig}.
 * <p>
 * Stages whose serde lands in v2 (currently only {@code TRANSFORM_MAP}) carry a {@code null}
 * factory; any attempt to deserialise them fails fast.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public enum StageKind {

    /* ---- Source ---- */
    SOURCE_URL(
        "URL Source",
        "() -> RAW_*",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("url", FieldType.STRING, "URL", "https://example.com/page"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "RAW_HTML / RAW_XML / RAW_JSON / RAW_TEXT")
        ),
        UrlSource::fromConfig
    ),

    SOURCE_PASTE(
        "Paste Source",
        "() -> RAW_*",
        StageCategory.SOURCE,
        List.of(
            new FieldSpec("body", FieldType.STRING, "Body", "<inline content>"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "RAW_HTML / RAW_XML / RAW_JSON / RAW_TEXT")
        ),
        PasteSource::fromConfig
    ),

    /* ---- Filter ---- */
    FILTER_DOM_TEXT_CONTAINS(
        "Text contains",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(new FieldSpec("needle", FieldType.STRING, "Text contains", "Dmg")),
        cfg -> DomTextContainsFilter.of(cfg.getString("needle"))
    ),

    FILTER_DOM_TEXT_MATCHES(
        "Text matches regex",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Text matches regex", "\\d+")
        ),
        cfg -> DomTextMatchesFilter.of(cfg.getString("regex"))
    ),

    FILTER_DOM_HAS_ATTR(
        "Has attribute",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("attributeName", FieldType.STRING, "Attribute name", "class"),
            new FieldSpec("expectedValue", FieldType.STRING, "Expected value (optional)", "primary")
        ),
        cfg -> {
            String name = cfg.getString("attributeName");
            String value = cfg.getString("expectedValue");
            return value.isEmpty()
                ? DomHasAttrFilter.of(name)
                : DomHasAttrFilter.of(name, value);
        }
    ),

    FILTER_DOM_TAG_EQUALS(
        "Tag equals",
        "List<DOM_NODE> -> List<DOM_NODE>",
        StageCategory.FILTER_DOM,
        List.of(
            new FieldSpec("tagName", FieldType.STRING, "Tag", "a")
        ),
        cfg -> DomTagEqualsFilter.of(cfg.getString("tagName"))
    ),

    FILTER_STRING_CONTAINS(
        "Contains",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("needle", FieldType.STRING, "Contains", "foo")
        ),
        cfg -> StringContainsFilter.of(cfg.getString("needle"))
    ),

    FILTER_STRING_MATCHES(
        "Matches regex",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "^foo")
        ),
        cfg -> StringMatchesFilter.of(cfg.getString("regex"))
    ),

    FILTER_STRING_STARTS_WITH(
        "Starts with",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("prefix", FieldType.STRING, "Prefix", "foo")
        ),
        cfg -> StringStartsWithFilter.of(cfg.getString("prefix"))
    ),

    FILTER_STRING_ENDS_WITH(
        "Ends with",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("suffix", FieldType.STRING, "Suffix", "bar")
        ),
        cfg -> StringEndsWithFilter.of(cfg.getString("suffix"))
    ),

    FILTER_STRING_EQUALS(
        "Equals",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(
            new FieldSpec("target", FieldType.STRING, "Equals", "foo")
        ),
        cfg -> StringEqualsFilter.of(cfg.getString("target"))
    ),

    FILTER_STRING_NON_EMPTY(
        "Non-empty",
        "List<STRING> -> List<STRING>",
        StageCategory.FILTER_STRING,
        List.of(),
        cfg -> StringNonEmptyFilter.of()
    ),

    FILTER_JSON_HAS_FIELD(
        "Has field",
        "List<JSON_OBJECT> -> List<JSON_OBJECT>",
        StageCategory.FILTER_JSON,
        List.of(
            new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare")
        ),
        cfg -> JsonHasFieldFilter.of(cfg.getString("fieldName"))
    ),

    FILTER_JSON_FIELD_EQUALS(
        "Field equals",
        "List<JSON_OBJECT> -> List<JSON_OBJECT>",
        StageCategory.FILTER_JSON,
        List.of(
            new FieldSpec("fieldName", FieldType.STRING, "Field name", "rare"),
            new FieldSpec("expectedValue", FieldType.STRING, "Equals", "true")
        ),
        cfg -> JsonFieldEqualsFilter.of(cfg.getString("fieldName"), cfg.getString("expectedValue"))
    ),

    FILTER_INT_GREATER_THAN(
        "Int >",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.INT, "Threshold", "0")
        ),
        cfg -> IntGreaterThanFilter.of(cfg.getInt("threshold"))
    ),

    FILTER_INT_LESS_THAN(
        "Int <",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.INT, "Threshold", "0")
        ),
        cfg -> IntLessThanFilter.of(cfg.getInt("threshold"))
    ),

    FILTER_INT_IN_RANGE(
        "Int in [min, max]",
        "List<INT> -> List<INT>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.INT, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.INT, "Max (inclusive)", "100")
        ),
        cfg -> IntInRangeFilter.of(cfg.getInt("min"), cfg.getInt("max"))
    ),

    FILTER_LONG_GREATER_THAN(
        "Long >",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")
        ),
        cfg -> LongGreaterThanFilter.of(cfg.getLong("threshold"))
    ),

    FILTER_LONG_LESS_THAN(
        "Long <",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.LONG, "Threshold", "0")
        ),
        cfg -> LongLessThanFilter.of(cfg.getLong("threshold"))
    ),

    FILTER_LONG_IN_RANGE(
        "Long in [min, max]",
        "List<LONG> -> List<LONG>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.LONG, "Min (inclusive)", "0"),
            new FieldSpec("max", FieldType.LONG, "Max (inclusive)", "100")
        ),
        cfg -> LongInRangeFilter.of(cfg.getLong("min"), cfg.getLong("max"))
    ),

    FILTER_DOUBLE_GREATER_THAN(
        "Double >",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")
        ),
        cfg -> DoubleGreaterThanFilter.of(cfg.getDouble("threshold"))
    ),

    FILTER_DOUBLE_LESS_THAN(
        "Double <",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("threshold", FieldType.DOUBLE, "Threshold", "0.0")
        ),
        cfg -> DoubleLessThanFilter.of(cfg.getDouble("threshold"))
    ),

    FILTER_DOUBLE_IN_RANGE(
        "Double in [min, max]",
        "List<DOUBLE> -> List<DOUBLE>",
        StageCategory.FILTER_NUMERIC,
        List.of(
            new FieldSpec("min", FieldType.DOUBLE, "Min (inclusive)", "0.0"),
            new FieldSpec("max", FieldType.DOUBLE, "Max (inclusive)", "100.0")
        ),
        cfg -> DoubleInRangeFilter.of(cfg.getDouble("min"), cfg.getDouble("max"))
    ),

    FILTER_NOT_NULL(
        "Not null",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> NotNullFilter.of(cfg.getDataType("elementType"))
    ),

    FILTER_TAKE(
        "Take first N",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("count", FieldType.INT, "Count", "10")
        ),
        cfg -> TakeFilter.of(cfg.getDataType("elementType"), cfg.getInt("count"))
    ),

    FILTER_SKIP(
        "Skip first N",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("count", FieldType.INT, "Count", "10")
        ),
        cfg -> SkipFilter.of(cfg.getDataType("elementType"), cfg.getInt("count"))
    ),

    FILTER_INDEX_IN_RANGE(
        "Index in [from, to)",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING"),
            new FieldSpec("fromInclusive", FieldType.INT, "From (inclusive)", "0"),
            new FieldSpec("toExclusive", FieldType.INT, "To (exclusive)", "10")
        ),
        cfg -> IndexInRangeFilter.of(cfg.getDataType("elementType"), cfg.getInt("fromInclusive"), cfg.getInt("toExclusive"))
    ),

    FILTER_DISTINCT(
        "Distinct",
        "List<T> -> List<T>",
        StageCategory.FILTER_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> DistinctFilter.of(cfg.getDataType("elementType"))
    ),

    /* ---- Transform ---- */
    PARSE_HTML(
        "Parse HTML",
        "RAW_HTML -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> ParseHtmlTransform.of()
    ),

    PARSE_XML(
        "Parse XML",
        "RAW_XML -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> ParseXmlTransform.of()
    ),

    PARSE_JSON(
        "Parse JSON",
        "RAW_JSON -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> ParseJsonTransform.of()
    ),

    TRANSFORM_CSS_SELECT(
        "CSS select",
        "DOM_NODE -> List<DOM_NODE>",
        StageCategory.TRANSFORM_DOM,
        List.of(new FieldSpec("selector", FieldType.STRING, "Selector", "table.infobox tr")),
        cfg -> CssSelectTransform.of(cfg.getString("selector"))
    ),

    TRANSFORM_NODE_TEXT(
        "Node text",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> NodeTextTransform.of()
    ),

    TRANSFORM_NODE_ATTR(
        "Node attribute",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(new FieldSpec("attributeName", FieldType.STRING, "Attribute name", "href")),
        cfg -> NodeAttrTransform.of(cfg.getString("attributeName"))
    ),

    TRANSFORM_NTH_CHILD(
        "Nth child",
        "DOM_NODE -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(
            new FieldSpec("childSelector", FieldType.STRING, "Child selector", "td"),
            new FieldSpec("index", FieldType.INT, "Index (0-based)", "0")
        ),
        cfg -> NthChildTransform.of(cfg.getString("childSelector"), cfg.getInt("index"))
    ),

    TRANSFORM_JSON_PATH(
        "JSON path",
        "JSON_ELEMENT -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(new FieldSpec("path", FieldType.STRING, "Path", "stats.combat.dmg")),
        cfg -> JsonPathTransform.of(cfg.getString("path"))
    ),

    TRANSFORM_JSON_FIELD(
        "JSON field",
        "JSON_OBJECT -> JSON_ELEMENT",
        StageCategory.TRANSFORM_JSON,
        List.of(new FieldSpec("fieldName", FieldType.STRING, "Field name", "stats")),
        cfg -> JsonFieldTransform.of(cfg.getString("fieldName"))
    ),

    TRANSFORM_REGEX_EXTRACT(
        "Regex extract",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "\\d+"),
            new FieldSpec("group", FieldType.INT, "Capture group", "0")
        ),
        cfg -> RegexExtractTransform.of(cfg.getString("regex"), cfg.getInt("group"))
    ),

    TRANSFORM_PARSE_INT(
        "Parse int",
        "STRING -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseIntTransform.of()
    ),

    TRANSFORM_PARSE_LONG(
        "Parse long",
        "STRING -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseLongTransform.of()
    ),

    TRANSFORM_PARSE_FLOAT(
        "Parse float",
        "STRING -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseFloatTransform.of()
    ),

    TRANSFORM_PARSE_DOUBLE(
        "Parse double",
        "STRING -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseDoubleTransform.of()
    ),

    TRANSFORM_PARSE_BOOLEAN(
        "Parse boolean",
        "STRING -> BOOLEAN",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> ParseBooleanTransform.of()
    ),

    TRANSFORM_TRIM(
        "Trim",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> TrimTransform.of()
    ),

    TRANSFORM_REPLACE(
        "Replace regex",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", "\\s+"),
            new FieldSpec("replacement", FieldType.STRING, "Replacement", "_")
        ),
        cfg -> ReplaceTransform.of(cfg.getString("regex"), cfg.getString("replacement"))
    ),

    TRANSFORM_SPLIT(
        "Split on regex",
        "STRING -> List<STRING>",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("regex", FieldType.STRING, "Regex", ",")
        ),
        cfg -> SplitTransform.of(cfg.getString("regex"))
    ),

    TRANSFORM_MAP(
        "Map sub-pipeline",
        "List<X> -> List<Y>",
        StageCategory.TRANSFORM_LIST,
        List.of(),
        null
    ),

    TRANSFORM_LOWERCASE(
        "Lowercase",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> LowerCaseTransform.of()
    ),

    TRANSFORM_UPPERCASE(
        "Uppercase",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> UpperCaseTransform.of()
    ),

    TRANSFORM_STRING_LENGTH(
        "String length",
        "STRING -> INT",
        StageCategory.TRANSFORM_STRING,
        List.of(),
        cfg -> StringLengthTransform.of()
    ),

    TRANSFORM_PREFIX(
        "Prefix",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("prefix", FieldType.STRING, "Prefix", ">>>")
        ),
        cfg -> PrefixTransform.of(cfg.getString("prefix"))
    ),

    TRANSFORM_SUFFIX(
        "Suffix",
        "STRING -> STRING",
        StageCategory.TRANSFORM_STRING,
        List.of(
            new FieldSpec("suffix", FieldType.STRING, "Suffix", "<<<")
        ),
        cfg -> SuffixTransform.of(cfg.getString("suffix"))
    ),

    TRANSFORM_LIST_LENGTH(
        "List length",
        "List<T> -> INT",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ListLengthTransform.of(cfg.getDataType("elementType"))
    ),

    TRANSFORM_REVERSE(
        "Reverse list",
        "List<T> -> List<T>",
        StageCategory.TRANSFORM_LIST,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ReverseTransform.of(cfg.getDataType("elementType"))
    ),

    TRANSFORM_ABS_INT(
        "Abs int",
        "INT -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsIntTransform.of()
    ),

    TRANSFORM_ABS_LONG(
        "Abs long",
        "LONG -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsLongTransform.of()
    ),

    TRANSFORM_ABS_FLOAT(
        "Abs float",
        "FLOAT -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsFloatTransform.of()
    ),

    TRANSFORM_ABS_DOUBLE(
        "Abs double",
        "DOUBLE -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> AbsDoubleTransform.of()
    ),

    TRANSFORM_NEGATE_INT(
        "Negate int",
        "INT -> INT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateIntTransform.of()
    ),

    TRANSFORM_NEGATE_LONG(
        "Negate long",
        "LONG -> LONG",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateLongTransform.of()
    ),

    TRANSFORM_NEGATE_FLOAT(
        "Negate float",
        "FLOAT -> FLOAT",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateFloatTransform.of()
    ),

    TRANSFORM_NEGATE_DOUBLE(
        "Negate double",
        "DOUBLE -> DOUBLE",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(),
        cfg -> NegateDoubleTransform.of()
    ),

    TRANSFORM_TO_STRING(
        "To string",
        "T -> STRING",
        StageCategory.TRANSFORM_PRIMITIVE,
        List.of(
            new FieldSpec("inputType", FieldType.DATA_TYPE, "Input type", "INT")
        ),
        cfg -> ToStringTransform.of(cfg.getDataType("inputType"))
    ),

    TRANSFORM_DOM_CHILDREN(
        "DOM children",
        "DOM_NODE -> List<DOM_NODE>",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomChildrenTransform.of()
    ),

    TRANSFORM_DOM_PARENT(
        "DOM parent",
        "DOM_NODE -> DOM_NODE",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomParentTransform.of()
    ),

    TRANSFORM_DOM_OUTER_HTML(
        "DOM outerHtml",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomOuterHtmlTransform.of()
    ),

    TRANSFORM_DOM_OWN_TEXT(
        "DOM ownText",
        "DOM_NODE -> STRING",
        StageCategory.TRANSFORM_DOM,
        List.of(),
        cfg -> DomOwnTextTransform.of()
    ),

    TRANSFORM_JSON_AS_STRING(
        "JSON as string",
        "JSON_ELEMENT -> STRING",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsStringTransform.of()
    ),

    TRANSFORM_JSON_AS_INT(
        "JSON as int",
        "JSON_ELEMENT -> INT",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsIntTransform.of()
    ),

    TRANSFORM_JSON_AS_LONG(
        "JSON as long",
        "JSON_ELEMENT -> LONG",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsLongTransform.of()
    ),

    TRANSFORM_JSON_AS_DOUBLE(
        "JSON as double",
        "JSON_ELEMENT -> DOUBLE",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsDoubleTransform.of()
    ),

    TRANSFORM_JSON_AS_BOOLEAN(
        "JSON as boolean",
        "JSON_ELEMENT -> BOOLEAN",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonAsBooleanTransform.of()
    ),

    TRANSFORM_JSON_STRINGIFY(
        "JSON stringify",
        "JSON_ELEMENT -> STRING",
        StageCategory.TRANSFORM_JSON,
        List.of(),
        cfg -> JsonStringifyTransform.of()
    ),

    TRANSFORM_BASE64_ENCODE(
        "Base64 encode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> Base64EncodeTransform.of()
    ),

    TRANSFORM_BASE64_DECODE(
        "Base64 decode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> Base64DecodeTransform.of()
    ),

    TRANSFORM_URL_ENCODE(
        "URL encode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> UrlEncodeTransform.of()
    ),

    TRANSFORM_URL_DECODE(
        "URL decode",
        "STRING -> STRING",
        StageCategory.TRANSFORM_ENCODING,
        List.of(),
        cfg -> UrlDecodeTransform.of()
    ),

    /* ---- Collect ---- */
    COLLECT_FIRST(
        "First",
        "List<T> -> T",
        StageCategory.COLLECT,
        List.of(new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "DOM_NODE")),
        cfg -> FirstCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_LAST(
        "Last",
        "List<T> -> T",
        StageCategory.COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> LastCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_LIST(
        "List",
        "List<T> -> List<T>",
        StageCategory.COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> ListCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_SET(
        "Set",
        "List<T> -> Set<T>",
        StageCategory.COLLECT,
        List.of(
            new FieldSpec("elementType", FieldType.DATA_TYPE, "Element type", "STRING")
        ),
        cfg -> SetCollect.of(cfg.getDataType("elementType"))
    ),

    COLLECT_JOIN(
        "Join",
        "List<STRING> -> STRING",
        StageCategory.COLLECT,
        List.of(
            new FieldSpec("separator", FieldType.STRING, "Separator", ", ")
        ),
        cfg -> JoinCollect.of(cfg.getString("separator"))
    ),

    /* ---- Compound ---- */
    BRANCH(
        "Branch",
        "I -> Map<String, Object>",
        StageCategory.BRANCH,
        List.of(
            new FieldSpec("inputType", FieldType.DATA_TYPE, "Input type", "STRING"),
            new FieldSpec("outputs", FieldType.SUB_PIPELINES_MAP, "Outputs", "")
        ),
        Branch::fromConfig
    ),

    PIPELINE_EMBED(
        "Embed pipeline",
        "() -> O",
        StageCategory.EMBED,
        List.of(
            new FieldSpec("embeddedPipelineId", FieldType.STRING, "Saved pipeline id", "wiki_dmg"),
            new FieldSpec("outputType", FieldType.DATA_TYPE, "Output type", "INT")
        ),
        cfg -> PipelineEmbed.of(cfg.getString("embeddedPipelineId"), cfg.getDataType("outputType"))
    );

    /** Cached snapshot of {@link #values()} reused by lookups to avoid the per-call defensive array clone. */
    private static final StageKind @NotNull [] CACHED_VALUES = values();

    private final @NotNull String displayName;
    private final @NotNull String description;
    private final @NotNull StageCategory category;
    private final @NotNull List<FieldSpec> schema;
    private final @Nullable Function<StageConfig, Stage<?, ?>> factory;

    /**
     * Returns every {@link StageKind} in {@code category}, in declaration order.
     *
     * @param category the category
     * @return the kinds in {@code category}, possibly empty
     */
    public static @NotNull List<StageKind> ofCategory(@NotNull StageCategory category) {
        return Arrays.stream(CACHED_VALUES)
            .filter(id -> id.category == category)
            .toList();
    }

    /**
     * Returns whether this kind needs a configuration modal (any non-empty schema).
     *
     * @return {@code true} when {@link #schema()} has at least one slot
     */
    public boolean requiresModal() {
        return !this.schema.isEmpty();
    }

}
