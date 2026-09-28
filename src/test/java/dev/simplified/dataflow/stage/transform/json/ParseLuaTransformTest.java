package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.encoding.HtmlDecodeTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParseLuaTransformTest {

    private static final @NotNull String MODULE = """
        --<pre>
        -- Reforge data. Copy the block below for a new entry.
        --[[
            [''] = { source = '', costs = { c = , u = } },
        --]]
        return {
            ['Armor'] = {
                ['Geometric'] = {
                    stats = {
                        c = {},
                        u = { fs = 1, trophy_chance = 1.5 },
                    },
                    source = 'Geometric Oddity',
                    costs = { u = 40000, r = 80000 },
                    tradeNpc = {'Hilda','Marthos'};
                },
            },
        }
        --</pre>
        """;

    private final PipelineContext ctx = PipelineContext.defaults();

    private @NotNull JsonElement parse(@NotNull String lua) {
        return ParseLuaTransform.of().execute(this.ctx, lua);
    }

    private static @NotNull JsonElement json(@NotNull String json) {
        return JsonParser.parseString(json);
    }

    private void assertRejects(@NotNull String lua) {
        assertThrows(IllegalArgumentException.class, () -> parse(lua));
    }

    private static @NotNull DataPipeline<JsonElement> pipeline() {
        return DataPipeline.builder()
            .source(LiteralSource.text("return &#123; name &#61; &#39;Hilda&#39;, level &#61; 3 &#125;"))
            .stage(HtmlDecodeTransform.of())
            .stage(ParseLuaTransform.of())
            .build();
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInput() {
        assertThat(ParseLuaTransform.of().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A data module with comments, nesting and mixed separators parses to its JSON form")
    void readsModule() {
        assertThat(parse(MODULE), is(equalTo(json("""
            {"Armor":{"Geometric":{"stats":{"c":[],"u":{"fs":1,"trophy_chance":1.5}},"source":"Geometric Oddity",\
            "costs":{"u":40000,"r":80000},"tradeNpc":["Hilda","Marthos"]}}}"""))));
    }

    @Test
    @DisplayName("An empty table becomes an empty array")
    void emptyTableIsArray() {
        assertThat(parse("return {}"), is(equalTo(new JsonArray())));
    }

    @Test
    @DisplayName("Positional values become an array in order")
    void positionalArray() {
        assertThat(parse("return { 'a', 'b', 'c' }"), is(equalTo(json("[\"a\",\"b\",\"c\"]"))));
    }

    @Test
    @DisplayName("Explicit keys 1..n become an array in index order, whatever their source order")
    void explicitIndexesInIndexOrder() {
        assertThat(parse("return { [2] = 'b', [1] = 'a' }"), is(equalTo(json("[\"a\",\"b\"]"))));
    }

    @Test
    @DisplayName("Named fields keep their source order")
    void namedFieldsKeepOrder() {
        assertThat(List.copyOf(parse("return { b = 1, a = 2, c = 3 }").getAsJsonObject().keySet()), contains("b", "a", "c"));
    }

    @Test
    @DisplayName("A bracketed string key may hold any text")
    void bracketedStringKey() {
        assertThat(parse("return { ['hub island'] = 1 }"), is(equalTo(json("{\"hub island\":1}"))));
    }

    @Test
    @DisplayName("Integer keys that are not 1..n become decimal string keys")
    void sparseIndexesBecomeObject() {
        assertThat(parse("return { [0] = 'z', [1] = 'a', [3] = 'c' }"), is(equalTo(json("{\"0\":\"z\",\"1\":\"a\",\"3\":\"c\"}"))));
    }

    @Test
    @DisplayName("Positional and named fields together become an object")
    void mixedFieldsBecomeObject() {
        assertThat(parse("return { 'a', name = 'x' }"), is(equalTo(json("{\"1\":\"a\",\"name\":\"x\"}"))));
    }

    @Test
    @DisplayName("A nil value omits its key")
    void nilOmitsKey() {
        assertThat(parse("return { a = 1, b = nil }"), is(equalTo(json("{\"a\":1}"))));
    }

    @Test
    @DisplayName("A positional nil still takes its index")
    void positionalNilTakesIndex() {
        assertThat(parse("return { 1, nil, 3 }"), is(equalTo(json("{\"1\":1,\"3\":3}"))));
    }

    @Test
    @DisplayName("A float key with an integer value is that integer")
    void integralFloatKey() {
        assertThat(parse("return { [1.0] = 'a', [2e0] = 'b' }"), is(equalTo(json("[\"a\",\"b\"]"))));
    }

    @Test
    @DisplayName("A boolean key becomes its name")
    void booleanKey() {
        assertThat(parse("return { [true] = 1 }"), is(equalTo(json("{\"true\":1}"))));
    }

    @Test
    @DisplayName("Integer literals stay integers and literals with a fraction or exponent are floats")
    void numberForms() {
        assertThat(parse("return { 3, 1.5, 2.0, 1e3, 0x1F, -4, - 5, .5, -0.25, 5. }").toString(), is(equalTo("[3,1.5,2.0,1000.0,31,-4,-5,0.5,-0.25,5.0]")));
    }

    @Test
    @DisplayName("A decimal integer too large for 64 bits reads as a float")
    void hugeIntegerIsFloat() {
        assertThat(parse("return { 12345678901234567890 }").getAsJsonArray().get(0).getAsDouble(), is(equalTo(1.2345678901234567E19)));
    }

    @Test
    @DisplayName("Booleans read as JSON booleans")
    void booleans() {
        assertThat(parse("return { true, false }"), is(equalTo(json("[true,false]"))));
    }

    @Test
    @DisplayName("Single- and double-quoted strings read alike")
    void quotedStrings() {
        assertThat(parse("return { 'single', \"double\" }"), is(equalTo(json("[\"single\",\"double\"]"))));
    }

    @Test
    @DisplayName("Character escapes decode")
    void characterEscapes() {
        assertThat(parse("return { 'a\\tb\\nc\\\\d\\'e\\\"f' }").getAsJsonArray().get(0).getAsString(), is(equalTo("a\tb\nc\\d'e\"f")));
    }

    @Test
    @DisplayName("Decimal escapes read as bytes of UTF-8")
    void decimalEscapes() {
        assertThat(parse("return { '\\65\\226\\156\\148' }").getAsJsonArray().get(0).getAsString(), is(equalTo("A\u2714")));
    }

    @Test
    @DisplayName("A backslash before any other character stands for that character, as in Lua 5.1")
    void unknownEscapeIsLiteral() {
        assertThat(parse("return { '\\(x\\) \\u003d' }").getAsJsonArray().get(0).getAsString(), is(equalTo("(x) u003d")));
    }

    @Test
    @DisplayName("A backslash before a line break reads as a line break")
    void escapedLineBreak() {
        assertThat(parse("return { 'a\\\r\nb' }").getAsJsonArray().get(0).getAsString(), is(equalTo("a\nb")));
    }

    @Test
    @DisplayName("Non-ASCII text in a string passes through")
    void nonAsciiText() {
        assertThat(parse("return { '\u2694 Combat' }").getAsJsonArray().get(0).getAsString(), is(equalTo("\u2694 Combat")));
    }

    @Test
    @DisplayName("A long string reads verbatim, without the line break after its opening bracket")
    void longString() {
        assertThat(parse("return { [[\nline 'one'\\n]] }").getAsJsonArray().get(0).getAsString(), is(equalTo("line 'one'\\n")));
    }

    @Test
    @DisplayName("A leveled long string ends only at its own closing bracket")
    void leveledLongString() {
        assertThat(parse("return { [==[a]]b]=]c]==] }").getAsJsonArray().get(0).getAsString(), is(equalTo("a]]b]=]c")));
    }

    @Test
    @DisplayName("Line breaks inside a long string read as \\n")
    void longStringLineBreaks() {
        assertThat(parse("return { [[a\r\nb\rc]] }").getAsJsonArray().get(0).getAsString(), is(equalTo("a\nb\nc")));
    }

    @Test
    @DisplayName("Block comments with levels are skipped")
    void leveledBlockComment() {
        assertThat(parse("--[==[ return { ]] } ]==] return { --[[ x ]] 1 }"), is(equalTo(json("[1]"))));
    }

    @Test
    @DisplayName("A semicolon after the returned table is allowed")
    void trailingSemicolon() {
        assertThat(parse("return { 1 };"), is(equalTo(json("[1]"))));
    }

    @Test
    @DisplayName("A local table can be returned by name")
    void returnsLocal() {
        assertThat(parse("local data = { a = 1 }\n\nreturn data"), is(equalTo(json("{\"a\":1}"))));
    }

    @Test
    @DisplayName("A table can name locals declared before it")
    void tableNamesLocals() {
        assertThat(
            parse("local slots = { 'ruby' }; local tiers = { rough = 1 }\nreturn { slots = slots, tiers = tiers }"),
            is(equalTo(json("{\"slots\":[\"ruby\"],\"tiers\":{\"rough\":1}}")))
        );
    }

    @Test
    @DisplayName("A local named twice yields two separate copies")
    void localReferencesAreCopies() {
        JsonObject result = parse("local x = { 1 } return { a = x, b = x }").getAsJsonObject();
        assertThat(result.get("a"), is(not(sameInstance(result.get("b")))));
    }

    @Test
    @DisplayName("A name no local before it declares throws")
    void undeclaredNameThrows() {
        assertRejects("return { a = data }");
    }

    @Test
    @DisplayName("A local cannot name itself in its own value")
    void selfReferenceThrows() {
        assertRejects("local data = { data } return data");
    }

    @Test
    @DisplayName("A function call throws")
    void functionCallThrows() {
        assertRejects("local Perks = require('Module:Mayor/Data') return { Perks }");
    }

    @Test
    @DisplayName("A concatenation throws")
    void concatenationThrows() {
        assertRejects("return { 'a' .. 'b' }");
    }

    @Test
    @DisplayName("A field access throws")
    void fieldAccessThrows() {
        assertRejects("local t = { a = 1 } return { t.a }");
    }

    @Test
    @DisplayName("A statement other than local before the return throws")
    void assignmentThrows() {
        assertRejects("data = {} return data");
    }

    @Test
    @DisplayName("A local declaring two names throws")
    void multipleLocalThrows() {
        assertRejects("local a, b = 1, 2 return { a }");
    }

    @Test
    @DisplayName("A local function throws")
    void localFunctionThrows() {
        assertRejects("local function f() end return {}");
    }

    @Test
    @DisplayName("A module without a return throws")
    void missingReturnThrows() {
        assertRejects("{ 1 }");
    }

    @Test
    @DisplayName("An empty module throws")
    void emptyModuleThrows() {
        assertRejects("");
    }

    @Test
    @DisplayName("A return of something other than a table throws")
    void returnOfStringThrows() {
        assertRejects("return 'x'");
    }

    @Test
    @DisplayName("Code after the returned table throws")
    void trailingCodeThrows() {
        assertRejects("return {} print(1)");
    }

    @Test
    @DisplayName("A table left open throws")
    void unclosedTableThrows() {
        assertRejects("return { 1, 2");
    }

    @Test
    @DisplayName("A string left open throws")
    void unfinishedStringThrows() {
        assertRejects("return { 'abc }");
    }

    @Test
    @DisplayName("A line break inside a quoted string throws")
    void lineBreakInStringThrows() {
        assertRejects("return { 'a\nb' }");
    }

    @Test
    @DisplayName("A long string left open throws")
    void unfinishedLongStringThrows() {
        assertRejects("return { [==[abc]] }");
    }

    @Test
    @DisplayName("A decimal escape above 255 throws")
    void decimalEscapeAbove255Throws() {
        assertRejects("return { '\\256' }");
    }

    @Test
    @DisplayName("A malformed number throws")
    void malformedNumberThrows() {
        assertRejects("return { 3abc }");
    }

    @Test
    @DisplayName("A hexadecimal float throws")
    void hexFloatThrows() {
        assertRejects("return { 0x1p4 }");
    }

    @Test
    @DisplayName("A hexadecimal integer beyond 64 bits throws")
    void hugeHexThrows() {
        assertRejects("return { 0xFFFFFFFFFFFFFFFF }");
    }

    @Test
    @DisplayName("A number JSON cannot hold throws")
    void infiniteNumberThrows() {
        assertRejects("return { 1e400 }");
    }

    @Test
    @DisplayName("A unary minus before anything but a number throws")
    void minusBeforeNameThrows() {
        assertRejects("return { -x }");
    }

    @Test
    @DisplayName("A key assigned twice in one table throws")
    void duplicateKeyThrows() {
        assertRejects("return { a = 1, a = 2 }");
    }

    @Test
    @DisplayName("A positional value on an index a bracketed key already took throws")
    void positionalOverExplicitThrows() {
        assertRejects("return { [1] = 'a', 'b' }");
    }

    @Test
    @DisplayName("Two keys sharing one JSON name throw")
    void jsonNameCollisionThrows() {
        assertRejects("return { [1] = 'a', ['1'] = 'b', x = 1 }");
    }

    @Test
    @DisplayName("A float key that is not an integer throws")
    void fractionalKeyThrows() {
        assertRejects("return { [1.5] = 'a' }");
    }

    @Test
    @DisplayName("A nil key throws")
    void nilKeyThrows() {
        assertRejects("return { [nil] = 'a' }");
    }

    @Test
    @DisplayName("A reserved word cannot name a field")
    void reservedFieldNameThrows() {
        assertRejects("return { end = 1 }");
    }

    @Test
    @DisplayName("Tables nested deeper than the limit throw")
    void tooDeepThrows() {
        String lua = "return " + "{".repeat(ParseLuaTransform.MAX_DEPTH + 1) + "}".repeat(ParseLuaTransform.MAX_DEPTH + 1);
        assertRejects(lua);
    }

    @Test
    @DisplayName("Tables nested to the limit parse")
    void deepestAllowedParses() {
        String lua = "return " + "{".repeat(ParseLuaTransform.MAX_DEPTH) + "}".repeat(ParseLuaTransform.MAX_DEPTH);
        assertThat(parse(lua).isJsonArray(), is(true));
    }

    @Test
    @DisplayName("The error names the line and column of the offending token")
    void errorNamesPosition() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> parse("return {\n  a = f()\n}"));
        assertThat(ex.getMessage(), startsWith("Malformed Lua at line 2, column 7:"));
    }

    @Test
    @DisplayName("An HTML-escaped module parses after HTML decoding")
    void htmlEscapedModule() {
        assertThat(pipeline().execute(), is(equalTo(json("{\"name\":\"Hilda\",\"level\":3}"))));
    }

    @Test
    @DisplayName("A pipeline round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline round-trips to the same output")
    void wireRoundTripExecutes() {
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(pipeline())).execute(), is(equalTo(pipeline().execute())));
    }

}
