package dev.simplified.dataflow.stage;

import dev.simplified.dataflow.stage.filter.list.DistinctByFilter;
import dev.simplified.dataflow.stage.filter.list.WhereFilter;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.predicate.common.ComparePredicate;
import dev.simplified.dataflow.stage.transform.dom.SpanExpandTransform;
import dev.simplified.dataflow.stage.transform.encoding.HtmlDecodeTransform;
import dev.simplified.dataflow.stage.transform.encoding.JsonUnescapeTransform;
import dev.simplified.dataflow.stage.transform.json.EntriesTransform;
import dev.simplified.dataflow.stage.transform.json.JoinByKeyTransform;
import dev.simplified.dataflow.stage.transform.json.KeyLookupTransform;
import dev.simplified.dataflow.stage.transform.json.ParseLuaTransform;
import dev.simplified.dataflow.stage.transform.json.ResolveAncestorTransform;
import dev.simplified.dataflow.stage.transform.list.BroadcastTransform;
import dev.simplified.dataflow.stage.transform.list.ConcatTransform;
import dev.simplified.dataflow.stage.transform.list.EnumerateTransform;
import dev.simplified.dataflow.stage.transform.list.GroupByTransform;
import dev.simplified.dataflow.stage.transform.list.RotateTransform;
import dev.simplified.dataflow.stage.transform.list.ZipTransform;
import dev.simplified.dataflow.stage.transform.primitive.ArithmeticDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.ArithmeticFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.ArithmeticIntTransform;
import dev.simplified.dataflow.stage.transform.primitive.ArithmeticLongTransform;
import dev.simplified.dataflow.stage.transform.primitive.BinaryArithmeticDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.BinaryArithmeticFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.BinaryArithmeticIntTransform;
import dev.simplified.dataflow.stage.transform.primitive.BinaryArithmeticLongTransform;
import dev.simplified.dataflow.stage.transform.primitive.CoalesceTransform;
import dev.simplified.dataflow.stage.transform.primitive.ConstantTransform;
import dev.simplified.dataflow.stage.transform.primitive.ExpectTransform;
import dev.simplified.dataflow.stage.transform.primitive.ParseRomanTransform;
import dev.simplified.dataflow.stage.transform.primitive.RoundDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.RoundFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.ToRawTransform;
import dev.simplified.dataflow.stage.transform.string.FetchTransform;
import dev.simplified.dataflow.stage.transform.string.RangeExpandTransform;
import dev.simplified.dataflow.stage.transform.string.ReplaceMatchTransform;
import dev.simplified.dataflow.stage.transform.string.ValueMapTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Pins, for each stage in its catalog, the wire id, the class that id resolves to and the palette
 * category the class declares.
 * <p>
 * A stage's id is the {@code "kind"} a stored pipeline carries, so each id here must keep
 * resolving through {@link StageRegistry#byId(String)} to the same class, and its
 * {@link StageSpec.Category} decides where a UI palette lists it.
 */
class StageCatalogTest {

    /**
     * One catalog row: the wire id, the class it resolves to, and the category that class declares.
     *
     * @param id the wire-format id
     * @param type the implementation class
     * @param category the declared palette category
     */
    private record Entry(@NotNull String id, @NotNull Class<?> type, @NotNull StageSpec.Category category) { }

    private static final @NotNull List<Entry> CATALOG = List.of(
        new Entry("TRANSFORM_JOIN_BY_KEY", JoinByKeyTransform.class, StageSpec.Category.TRANSFORM_JSON),
        new Entry("TRANSFORM_KEY_LOOKUP", KeyLookupTransform.class, StageSpec.Category.TRANSFORM_JSON),
        new Entry("TRANSFORM_RESOLVE_ANCESTOR", ResolveAncestorTransform.class, StageSpec.Category.TRANSFORM_JSON),
        new Entry("TRANSFORM_CONCAT", ConcatTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("TRANSFORM_FETCH", FetchTransform.class, StageSpec.Category.TRANSFORM_STRING),
        new Entry("TRANSFORM_TO_RAW", ToRawTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_CONSTANT", ConstantTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_COALESCE", CoalesceTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_PARSE_ROMAN", ParseRomanTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ARITHMETIC_INT", ArithmeticIntTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ARITHMETIC_LONG", ArithmeticLongTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ARITHMETIC_FLOAT", ArithmeticFloatTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ARITHMETIC_DOUBLE", ArithmeticDoubleTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_BINARY_ARITHMETIC_INT", BinaryArithmeticIntTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_BINARY_ARITHMETIC_LONG", BinaryArithmeticLongTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_BINARY_ARITHMETIC_FLOAT", BinaryArithmeticFloatTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_BINARY_ARITHMETIC_DOUBLE", BinaryArithmeticDoubleTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ROUND_FLOAT", RoundFloatTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_ROUND_DOUBLE", RoundDoubleTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE),
        new Entry("TRANSFORM_JSON_ENTRIES", EntriesTransform.class, StageSpec.Category.TRANSFORM_JSON),
        new Entry("PARSE_LUA", ParseLuaTransform.class, StageSpec.Category.TRANSFORM_JSON),
        new Entry("TRANSFORM_JSON_UNESCAPE", JsonUnescapeTransform.class, StageSpec.Category.TRANSFORM_ENCODING),
        new Entry("TRANSFORM_HTML_DECODE", HtmlDecodeTransform.class, StageSpec.Category.TRANSFORM_ENCODING),
        new Entry("TRANSFORM_DOM_SPAN_EXPAND", SpanExpandTransform.class, StageSpec.Category.TRANSFORM_DOM),
        new Entry("TRANSFORM_VALUE_MAP", ValueMapTransform.class, StageSpec.Category.TRANSFORM_STRING),
        new Entry("TRANSFORM_REPLACE_MATCH", ReplaceMatchTransform.class, StageSpec.Category.TRANSFORM_STRING),
        new Entry("TRANSFORM_RANGE_EXPAND", RangeExpandTransform.class, StageSpec.Category.TRANSFORM_STRING),
        new Entry("TRANSFORM_GROUP_BY", GroupByTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("FILTER_DISTINCT_BY", DistinctByFilter.class, StageSpec.Category.FILTER_LIST),
        new Entry("TRANSFORM_ENUMERATE", EnumerateTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("TRANSFORM_ZIP", ZipTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("TRANSFORM_ROTATE", RotateTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("TRANSFORM_BROADCAST", BroadcastTransform.class, StageSpec.Category.TRANSFORM_LIST),
        new Entry("FILTER_WHERE", WhereFilter.class, StageSpec.Category.FILTER_LIST),
        new Entry("PREDICATE_COMPARE", ComparePredicate.class, StageSpec.Category.PREDICATE_COMMON),
        new Entry("TRANSFORM_EXPECT", ExpectTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE)
    );

    @TestFactory
    Stream<DynamicTest> everyIdResolvesToItsClass() {
        return CATALOG.stream().map(entry -> DynamicTest.dynamicTest(
            entry.id(),
            () -> assertThat(StageRegistry.byId(entry.id()), is(sameInstance(entry.type())))
        ));
    }

    @TestFactory
    Stream<DynamicTest> everyIdCarriesItsCategory() {
        return CATALOG.stream().map(entry -> DynamicTest.dynamicTest(
            entry.id(),
            () -> assertThat(StageRegistry.byId(entry.id()).getAnnotation(StageSpec.class).category(), is(equalTo(entry.category())))
        ));
    }

    @TestFactory
    Stream<DynamicTest> everyCategoryListsItsStage() {
        return CATALOG.stream().map(entry -> DynamicTest.dynamicTest(
            entry.id(),
            () -> assertThat(
                StageRegistry.ofCategory(entry.category()).stream().map(Class::getName).toList(),
                hasItem(entry.type().getName())
            )
        ));
    }

}
