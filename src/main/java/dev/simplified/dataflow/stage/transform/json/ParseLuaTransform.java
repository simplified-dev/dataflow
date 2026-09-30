package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.transform.encoding.HtmlDecodeTransform;
import org.intellij.lang.annotations.PrintFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@link TransformStage} that parses a Lua data module - a {@code return} of a table built from
 * literals - into a Gson {@link JsonElement} tree.
 * <p>
 * It reads Lua as Lua 5.1 reads it, the version MediaWiki's Scribunto runs:
 * <ul>
 *   <li><b>tables</b> - positional, {@code name = value} and {@code [key] = value} fields separated
 *       by {@code ,} or {@code ;}, with an optional trailing separator</li>
 *   <li><b>strings</b> - single-quoted, double-quoted and long-bracket strings such as
 *       {@code [[...]]} and {@code [==[...]==]}; a quoted string decodes
 *       {@code \a \b \f \n \r \t \v}, a backslash before a line break and the decimal byte escape
 *       {@code \ddd}, whose bytes read as UTF-8, and a backslash before any other character stands
 *       for that character, so {@code \(} reads as {@code (} and {@code \x41} as {@code x41}</li>
 *   <li><b>numbers</b> - decimal integers and floats, hexadecimal integers, and one unary minus
 *       before either</li>
 *   <li><b>booleans</b> and <b>nil</b></li>
 *   <li><b>locals</b> - {@code local name = value} statements before the {@code return}, each
 *       value a literal or a table, and a name in value position, the {@code return} included,
 *       reading the value of the latest local of that name declared before it; the first read of
 *       a local takes its value and every later read a copy, so the tree returned never holds one
 *       table in two places, while a copy shares the local's strings, numbers and booleans, which
 *       cannot change</li>
 *   <li><b>comments</b> - line comments and long-bracket block comments, anywhere whitespace may
 *       stand</li>
 * </ul>
 * A table whose keys are exactly {@code 1..n} becomes a {@link JsonArray} in index order, an empty
 * table included, and any other table a {@link JsonObject} whose keys keep their source order.
 * An integer key becomes its decimal string and a boolean key {@code "true"} or {@code "false"}.
 * An integer literal becomes a JSON integer, and a literal with a fraction or an exponent a JSON
 * float. A decimal or hexadecimal integer too large for a signed 64-bit integer becomes a JSON
 * float too, the double Lua 5.1 reads it as. A {@code nil} value omits its key, and a positional
 * {@code nil} still takes its index, so {@code {1, nil, 3}} becomes an object keyed {@code "1"}
 * and {@code "3"}.
 * <p>
 * Anything outside that subset throws {@link IllegalArgumentException} naming the line and column,
 * as {@link ParseJsonTransform} throws on malformed JSON, so a module that starts computing values
 * fails instead of yielding half a table: a function call, a concatenation, a field access, a
 * name no earlier local declares, a statement other than {@code local} before the {@code return},
 * anything after the returned table, a {@code return} of something other than a table, a
 * hexadecimal float, a number JSON cannot hold, a {@code [[} inside a long string or comment that
 * {@code [[} opens, decimal escapes whose bytes are not UTF-8, a float key that is not an integer,
 * a key assigned twice in one table, two keys that would share one JSON name, and tables nested
 * deeper than {@value #MAX_DEPTH} levels, the levels of every local a table names counted in.
 * Reads of locals also throw once their copies together hold more values and string characters,
 * keys included, than the module has characters, so a few lines that each name the local before
 * them twice cannot double the tree once per line, and a long string named over and over cannot
 * multiply its length. A module transcluded through {@code msgnw} arrives HTML-escaped and needs
 * {@link HtmlDecodeTransform} first.
 */
@StageSpec(
    id = "PARSE_LUA",
    displayName = "Parse Lua",
    description = "STRING -> JSON_ELEMENT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ParseLuaTransform implements TransformStage<String, JsonElement> {

    /**
     * Deepest table nesting read before the module is refused, the syntax-level limit Lua itself
     * applies.
     */
    public static final int MAX_DEPTH = 200;

    /**
     * Constructs a parse-Lua stage.
     *
     * @return the stage
     */
    public static @NotNull ParseLuaTransform of() {
        return new ParseLuaTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonElement execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return new LuaReader(input).module();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonElement> outputType() {
        return DataTypes.JSON_ELEMENT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Parse as Lua";
    }

    /**
     * Recursive-descent reader over the source of one module, holding the read position.
     */
    private static final class LuaReader {

        private static final @NotNull Set<String> RESERVED = Set.of(
            "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "if", "in",
            "local", "nil", "not", "or", "repeat", "return", "then", "true", "until", "while"
        );

        private static final @NotNull Pattern LINE_BREAK = Pattern.compile("\r\n|\n\r|\r|\n");

        private final @NotNull String source;

        /**
         * Locals declared so far, by name.
         */
        private final @NotNull Map<String, Local> locals = new HashMap<>();

        private int position;

        private int depth;

        /**
         * Values and string characters the repeated reads of locals have copied so far.
         */
        private long copied;

        /**
         * Position of the first decimal escape whose byte is still pending.
         */
        private int bytesAt;

        LuaReader(@NotNull String source) {
            this.source = source;
        }

        /**
         * Reads the whole module: any {@code local} statements, {@code return} and one table, an
         * optional semicolon and nothing else.
         *
         * @return the returned table as JSON
         */
        @NotNull JsonElement module() {
            skipTrivia();

            while (atWord("local"))
                readLocal();

            if (!atWord("return"))
                throw error(this.position, "expected 'local' or 'return' but found %s", describe(this.position));

            this.position += "return".length();
            skipTrivia();
            int valueAt = this.position;
            JsonElement table = readValue().element();

            if (table == null || table.isJsonPrimitive())
                throw error(valueAt, "expected a table after 'return' but found '%s'", table == null ? "nil" : table);

            skipTrivia();

            if (peek() == ';') {
                this.position++;
                skipTrivia();
            }

            if (this.position < this.source.length())
                throw error(this.position, "expected the end of the module but found %s", describe(this.position));

            return table;
        }

        /**
         * Reads one table constructor, the position at its opening brace, measuring it from the
         * measures of its fields.
         *
         * @return the table as a JSON array or object, with its measures
         */
        private @NotNull Value readTable() {
            int open = this.position++;

            if (++this.depth > MAX_DEPTH)
                throw error(open, "tables nest deeper than %s levels", MAX_DEPTH);

            Map<Object, JsonElement> fields = new LinkedHashMap<>();
            Set<Object> assigned = new HashSet<>();
            long nextIndex = 1;
            int height = 0;
            long size = 1;
            skipTrivia();

            while (peek() != '}') {
                int fieldAt = this.position;
                Object key;

                if (peek() == '[' && !atLongBracket())
                    key = readBracketKey();
                else if (atNamedField())
                    key = readFieldName();
                else
                    key = nextIndex++;

                Value value = readValue();

                if (!assigned.add(key))
                    throw error(fieldAt, "key '%s' is assigned twice in one table", key);

                if (value.element() != null) {
                    fields.put(key, value.element());
                    height = Math.max(height, value.height());
                    size += value.size() + (key instanceof String text ? text.length() : 0);
                }

                skipTrivia();
                int separator = peek();

                if (separator == ',' || separator == ';') {
                    this.position++;
                    skipTrivia();
                } else if (separator != '}')
                    throw error(this.position, "expected ',', ';' or '}' after a table field but found %s", describe(this.position));
            }

            this.position++;
            this.depth--;
            return new Value(toJson(fields, open), height + 1, size);
        }

        /**
         * Reads a {@code [key] =} prefix, the position at its opening bracket.
         *
         * @return the key as a {@link Long}, {@link String} or {@link Boolean}
         */
        private @NotNull Object readBracketKey() {
            this.position++;
            skipTrivia();
            int keyAt = this.position;
            JsonElement key = readValue().element();

            if (key == null)
                throw error(keyAt, "a table key cannot be nil");

            if (!key.isJsonPrimitive())
                throw error(keyAt, "a table key must be a string, a number or a boolean, not a table");

            skipTrivia();
            expect(']');
            skipTrivia();
            expect('=');
            return keyOf(key.getAsJsonPrimitive(), keyAt);
        }

        /**
         * Reads a {@code name =} prefix, the position at the name.
         *
         * @return the name
         */
        private @NotNull String readFieldName() {
            int nameAt = this.position;
            String name = readName();

            if (RESERVED.contains(name))
                throw error(nameAt, "reserved word '%s' cannot name a field", name);

            skipTrivia();
            this.position++;
            return name;
        }

        /**
         * Reads one value.
         *
         * @return the value with its measures, its element {@code null} for {@code nil}
         */
        private @NotNull Value readValue() {
            skipTrivia();
            int at = this.position;
            int c = peek();

            if (c == '{') return readTable();
            if (c == '"' || c == '\'') return Value.of(new JsonPrimitive(readQuoted()));
            if (c == '[' && atLongBracket()) return Value.of(new JsonPrimitive(readLongBracket("string")));
            if (atNumber()) return Value.of(readNumber());

            if (c == '-') {
                this.position++;
                skipTrivia();

                if (!atNumber())
                    throw error(this.position, "expected a number after '-' but found %s", describe(this.position));

                return Value.of(negate(readNumber()));
            }

            if (isNameStart(c)) {
                String name = readName();

                return switch (name) {
                    case "true" -> Value.of(new JsonPrimitive(true));
                    case "false" -> Value.of(new JsonPrimitive(false));
                    case "nil" -> Value.NIL;
                    default -> readLocalValue(name, at);
                };
            }

            throw error(at, "expected a value but found %s", describe(at));
        }

        /**
         * Reads one {@code local name = value} statement and its optional semicolon, the position
         * at {@code local}.
         */
        private void readLocal() {
            this.position += "local".length();
            skipTrivia();
            int nameAt = this.position;

            if (!isNameStart(peek()) || RESERVED.contains(nameAt(nameAt)))
                throw error(nameAt, "expected a name after 'local' but found %s", describe(nameAt));

            String name = readName();
            skipTrivia();

            if (peek() != '=' || peek(1) == '=')
                throw error(this.position, "expected '=' after 'local %s' but found %s", name, describe(this.position));

            this.position++;
            this.locals.put(name, new Local(readValue()));
            skipTrivia();

            if (peek() == ';') {
                this.position++;
                skipTrivia();
            }
        }

        /**
         * Reads the value of the latest local named {@code name}. The first read takes the value
         * and every later read a copy of it, so the tree returned never holds one table in two
         * places, and the copies together hold no more values and string characters than the
         * module has characters.
         *
         * @param name the name read
         * @param at the name's position, for the error message
         * @return the local's value with its measures, its element {@code null} when it is
         *         {@code nil}
         */
        private @NotNull Value readLocalValue(@NotNull String name, int at) {
            Local local = this.locals.get(name);

            if (local == null)
                throw error(at, "found the name '%s', which no local before it declares", name);

            Value value = local.value;

            if (value.element() == null) return value;

            if (this.depth + value.height() > MAX_DEPTH)
                throw error(at, "tables nest deeper than %s levels", MAX_DEPTH);

            if (!local.taken) {
                local.taken = true;
                return value;
            }

            this.copied += value.size();

            if (this.copied > this.source.length())
                throw error(at, "the local '%s' is read so often that its copies outgrow the module", name);

            return new Value(value.element().deepCopy(), value.height(), value.size());
        }

        /**
         * Reads a quoted string, the position at its opening quote.
         *
         * @return the decoded string
         */
        private @NotNull String readQuoted() {
            int open = this.position;
            char quote = this.source.charAt(this.position++);
            int start = this.position;

            while (peek() != quote && peek() != '\\') {
                if (atLineEnd())
                    throw error(open, "unfinished string");

                this.position++;
            }

            if (peek() == quote)
                return this.source.substring(start, this.position++);

            StringBuilder text = new StringBuilder(this.source.substring(start, this.position));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            while (peek() != quote) {
                if (atLineEnd())
                    throw error(open, "unfinished string");

                if (peek() == '\\') {
                    readEscape(text, bytes);
                    continue;
                }

                flush(text, bytes);
                text.append(this.source.charAt(this.position++));
            }

            flush(text, bytes);
            this.position++;
            return text.toString();
        }

        /**
         * Reads one escape of a quoted string, the position at its backslash. A decimal escape
         * above 127 is held in {@code bytes} until the escapes after it complete its UTF-8
         * sequence; every other escape is appended to {@code text}.
         *
         * @param text the decoded string so far
         * @param bytes the pending bytes of decimal escapes above 127
         */
        private void readEscape(@NotNull StringBuilder text, @NotNull ByteArrayOutputStream bytes) {
            int escapeAt = this.position++;
            int c = peek();

            if (isDigit(c)) {
                int value = 0;

                for (int digits = 0; digits < 3 && isDigit(peek()); digits++)
                    value = value * 10 + (this.source.charAt(this.position++) - '0');

                if (value > 255)
                    throw error(escapeAt, "decimal escape '%s' is above 255", this.source.substring(escapeAt, this.position));

                if (value > 127) {
                    if (bytes.size() == 0)
                        this.bytesAt = escapeAt;

                    bytes.write(value);
                } else
                    flush(text, bytes).append((char) value);

                return;
            }

            flush(text, bytes);

            if (c == '\n' || c == '\r') {
                skipLineBreak();
                text.append('\n');
                return;
            }

            char decoded = switch (c) {
                case 'a' -> (char) 7;
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'v' -> (char) 11;
                default -> {
                    if (c < 0)
                        throw error(escapeAt, "unfinished string");

                    yield (char) c;
                }
            };

            text.append(decoded);
            this.position++;
        }

        /**
         * Reads a long-bracket string or comment body, the position at its opening bracket. The
         * line break straight after the opening bracket is dropped, and every line break inside
         * reads as {@code \n}, as Lua reads them. A body that {@code [[} opens holds no other
         * {@code [[}, which Lua 5.1 refuses as nesting.
         *
         * @param what what the bracket opens, for the error message
         * @return the body
         */
        private @NotNull String readLongBracket(@NotNull String what) {
            int open = this.position;
            int level = longBracketLevel();
            this.position += level + 2;
            skipLineBreak();
            String close = "]" + "=".repeat(level) + "]";
            int end = this.source.indexOf(close, this.position);
            int nested = level == 0 ? this.source.indexOf("[[", this.position) : -1;

            if (nested >= 0 && (end < 0 || nested < end))
                throw error(nested, "a long %s that '[[' opens cannot hold another '[['", what);

            if (end < 0)
                throw error(open, "unfinished long %s", what);

            String body = this.source.substring(this.position, end);
            this.position = end + close.length();
            return body.indexOf('\r') < 0 ? body : LINE_BREAK.matcher(body).replaceAll("\n");
        }

        /**
         * Reads a number literal without its sign.
         *
         * @return the number as a JSON integer or float
         */
        private @NotNull JsonPrimitive readNumber() {
            int start = this.position;

            if (peek() == '0' && (peek(1) == 'x' || peek(1) == 'X')) {
                this.position += 2;

                while (isHexDigit(peek()))
                    this.position++;

                if (this.position == start + 2 || isNumeralPart(peek()))
                    throw error(start, "malformed number '%s'", numeral(start));

                int significant = start + 2;

                while (significant < this.position - 1 && this.source.charAt(significant) == '0')
                    significant++;

                String hex = this.source.substring(significant, this.position);

                try {
                    return new JsonPrimitive(Long.parseLong(hex, 16));
                } catch (NumberFormatException ignored) { }

                // a leading digit worth 2^1024 or more is past every double
                double value = (hex.length() - 1) * 4L > Double.MAX_EXPONENT
                    ? Double.POSITIVE_INFINITY
                    : new BigInteger(hex, 16).doubleValue();

                if (Double.isInfinite(value))
                    throw error(start, "number '%s' has no JSON form", numeral(start));

                return new JsonPrimitive(value);
            }

            skipDigits();
            boolean fractional = false;

            if (peek() == '.') {
                this.position++;
                skipDigits();
                fractional = true;
            }

            if (peek() == 'e' || peek() == 'E') {
                this.position++;

                if (peek() == '+' || peek() == '-')
                    this.position++;

                if (!skipDigits())
                    throw error(start, "malformed number '%s'", numeral(start));

                fractional = true;
            }

            if (isNumeralPart(peek()))
                throw error(start, "malformed number '%s'", numeral(start));

            String text = this.source.substring(start, this.position);

            if (!fractional) {
                try {
                    return new JsonPrimitive(Long.parseLong(text));
                } catch (NumberFormatException ignored) { }
            }

            double value = Double.parseDouble(text);

            if (Double.isInfinite(value))
                throw error(start, "number '%s' has no JSON form", text);

            return new JsonPrimitive(value);
        }

        /**
         * Builds the JSON form of a table's fields.
         *
         * @param fields the fields in source order, {@code nil} ones left out
         * @param open the position of the table's opening brace, for the error message
         * @return a JSON array when the keys are exactly {@code 1..n}, otherwise a JSON object
         */
        private @NotNull JsonElement toJson(@NotNull Map<Object, JsonElement> fields, int open) {
            int size = fields.size();

            if (fields.keySet().stream().allMatch(key -> key instanceof Long index && index >= 1 && index <= size)) {
                JsonElement[] slots = new JsonElement[size];
                fields.forEach((key, value) -> slots[(int) ((Long) key - 1)] = value);
                JsonArray array = new JsonArray(size);

                for (JsonElement slot : slots)
                    array.add(slot);

                return array;
            }

            JsonObject object = new JsonObject();

            for (Map.Entry<Object, JsonElement> field : fields.entrySet()) {
                String name = String.valueOf(field.getKey());

                if (object.has(name))
                    throw error(open, "two keys of the table share the JSON name '%s'", name);

                object.add(name, field.getValue());
            }

            return object;
        }

        /**
         * Normalises a bracketed key the way Lua compares keys: a float with an integer value is
         * that integer.
         *
         * @param key the key literal
         * @param at the key's position, for the error message
         * @return the key as a {@link Long}, {@link String} or {@link Boolean}
         */
        private @NotNull Object keyOf(@NotNull JsonPrimitive key, int at) {
            if (key.isString()) return key.getAsString();
            if (key.isBoolean()) return key.getAsBoolean();
            Number number = key.getAsNumber();
            if (number instanceof Long integer) return integer;
            double value = number.doubleValue();

            if (value != Math.rint(value) || Math.abs(value) >= 0x1p63)
                throw error(at, "float key '%s' is not a 64-bit integer and has no JSON name", number);

            return (long) value;
        }

        private @NotNull JsonPrimitive negate(@NotNull JsonPrimitive number) {
            return number.getAsNumber() instanceof Long integer
                ? new JsonPrimitive(-integer)
                : new JsonPrimitive(-number.getAsDouble());
        }

        /**
         * Appends the pending bytes of decimal escapes to {@code text} as the UTF-8 they spell.
         *
         * @param text the decoded string so far
         * @param bytes the pending bytes, emptied here
         * @return {@code text}
         * @throws IllegalArgumentException when the bytes are not UTF-8
         */
        private @NotNull StringBuilder flush(@NotNull StringBuilder text, @NotNull ByteArrayOutputStream bytes) {
            if (bytes.size() > 0) {
                try {
                    text.append(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes.toByteArray())));
                } catch (CharacterCodingException ex) {
                    throw error(this.bytesAt, "decimal escapes spell bytes that are not UTF-8");
                }

                bytes.reset();
            }

            return text;
        }

        private void skipTrivia() {
            while (true) {
                int c = peek();

                if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 11)
                    this.position++;
                else if (c == '-' && peek(1) == '-')
                    skipComment();
                else
                    return;
            }
        }

        private void skipComment() {
            this.position += 2;

            if (atLongBracket()) {
                readLongBracket("comment");
                return;
            }

            while (this.position < this.source.length() && !atLineEnd())
                this.position++;
        }

        private void skipLineBreak() {
            int c = peek();
            if (c != '\n' && c != '\r') return;
            this.position++;
            int next = peek();
            if ((next == '\n' || next == '\r') && next != c) this.position++;
        }

        private boolean skipDigits() {
            int start = this.position;

            while (isDigit(peek()))
                this.position++;

            return this.position > start;
        }

        private @NotNull String readName() {
            int start = this.position;

            while (isNamePart(peek()))
                this.position++;

            return this.source.substring(start, this.position);
        }

        private void expect(char expected) {
            if (peek() != expected)
                throw error(this.position, "expected '%s' but found %s", expected, describe(this.position));

            this.position++;
        }

        private boolean atNamedField() {
            if (!isNameStart(peek())) return false;
            int start = this.position;
            readName();
            skipTrivia();
            boolean named = peek() == '=' && peek(1) != '=';
            this.position = start;
            return named;
        }

        private boolean atWord(@NotNull String word) {
            return this.source.startsWith(word, this.position) && !isNamePart(peek(word.length()));
        }

        private boolean atNumber() {
            return isDigit(peek()) || (peek() == '.' && isDigit(peek(1)));
        }

        private boolean atLongBracket() {
            return longBracketLevel() >= 0;
        }

        private boolean atLineEnd() {
            int c = peek();
            return c < 0 || c == '\n' || c == '\r';
        }

        /**
         * Counts the level of the long bracket opening at the position.
         *
         * @return the number of {@code =} between its brackets, or {@code -1} when no long bracket
         *         opens here
         */
        private int longBracketLevel() {
            if (peek() != '[') return -1;
            int level = 0;

            while (peek(level + 1) == '=')
                level++;

            return peek(level + 1) == '[' ? level : -1;
        }

        private int peek() {
            return peek(0);
        }

        private int peek(int offset) {
            int index = this.position + offset;
            return index < this.source.length() ? this.source.charAt(index) : -1;
        }

        private @NotNull String numeral(int start) {
            int end = start;

            while (end < this.source.length() && isNumeralPart(this.source.charAt(end)))
                end++;

            return this.source.substring(start, end);
        }

        private @NotNull String describe(int at) {
            if (at >= this.source.length()) return "the end of the input";
            if (isNameStart(this.source.charAt(at))) return "'" + nameAt(at) + "'";
            if (this.source.startsWith("..", at)) return "'..'";
            return "'" + Character.toString(this.source.codePointAt(at)) + "'";
        }

        private @NotNull String nameAt(int at) {
            int end = at;

            while (end < this.source.length() && isNamePart(this.source.charAt(end)))
                end++;

            return this.source.substring(at, end);
        }

        private @NotNull IllegalArgumentException error(int at, @PrintFormat @NotNull String detail, @Nullable Object... args) {
            int line = 1;
            int lineStart = 0;

            for (int index = 0; index < at && index < this.source.length(); index++) {
                char c = this.source.charAt(index);

                if (c == '\n' || (c == '\r' && (index + 1 >= this.source.length() || this.source.charAt(index + 1) != '\n'))) {
                    line++;
                    lineStart = index + 1;
                }
            }

            return new IllegalArgumentException(String.format(
                "Malformed Lua at line %s, column %s: %s", line, at - lineStart + 1, String.format(detail, args)
            ));
        }

        private static boolean isDigit(int c) {
            return c >= '0' && c <= '9';
        }

        private static boolean isHexDigit(int c) {
            return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        }

        private static boolean isNameStart(int c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        }

        private static boolean isNamePart(int c) {
            return isNameStart(c) || isDigit(c);
        }

        private static boolean isNumeralPart(int c) {
            return isNamePart(c) || c == '.';
        }

        /**
         * A declared local, with the measures each read of it is checked against.
         */
        private static final class Local {

            /**
             * Value of the local with its measures, its element {@code null} when it is
             * {@code nil}.
             */
            private final @NotNull Value value;

            /**
             * Whether a read has taken the value itself, so every later read copies it.
             */
            private boolean taken;

            Local(@NotNull Value value) {
                this.value = value;
            }

        }

        /**
         * A value read, with the measures a read of a local holding it is checked against, taken
         * as the value is read so that no value is walked to measure it.
         *
         * @param element the value as JSON, or {@code null} for {@code nil}
         * @param height the levels of tables in the value, {@code 0} when it holds no table
         * @param size the values in the value, itself included, plus the characters of every
         *             string and string key in it
         */
        private record Value(@Nullable JsonElement element, int height, long size) {

            /**
             * The value of {@code nil}.
             */
            static final @NotNull Value NIL = new Value(null, 0, 0);

            /**
             * Measures a string, number or boolean.
             *
             * @param primitive the value
             * @return the value with its measures
             */
            static @NotNull Value of(@NotNull JsonPrimitive primitive) {
                return new Value(primitive, 0, 1 + (primitive.isString() ? primitive.getAsString().length() : 0));
            }

        }

    }

}
