package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.primitive.ParseIntTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link ReplaceMatchTransform}: the in-place rewrite of each match's group, the cases
 * that leave a match unchanged, the factory's refusals and the wire round trip.
 */
class ReplaceMatchTransformTest {

    private static final @NotNull String KEYWORD = "%\\{KEYWORD:([^}]+)\\}";

    private final @NotNull PipelineContext ctx = PipelineContext.defaults();

    private static @NotNull List<Stage<?, ?>> upper() {
        return List.of(UpperCaseTransform.of());
    }

    private static @NotNull List<Stage<?, ?>> constantCase() {
        return List.of(SnakeCaseTransform.of(), UpperCaseTransform.of());
    }

    private static @NotNull DataPipeline<String> pipeline(@NotNull String value, @NotNull ReplaceMatchTransform stage) {
        return DataPipeline.builder()
            .source(LiteralSource.text(value))
            .stage(stage)
            .build();
    }

    @Test
    @DisplayName("A null input yields null")
    void nullInYieldsNull() {
        assertThat(ReplaceMatchTransform.of("[a-z]+", null, upper()).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("A group's text runs through the body and replaces the group in place")
    void rewritesGroupInPlace() {
        ReplaceMatchTransform stage = ReplaceMatchTransform.of(KEYWORD, 1, constantCase());
        assertThat(
            stage.execute(this.ctx, "Grants %{KEYWORD:Heart of the Mountain} access"),
            is(equalTo("Grants %{KEYWORD:HEART_OF_THE_MOUNTAIN} access"))
        );
    }

    @Test
    @DisplayName("Every match is rewritten, left to right, with the text between them kept")
    void rewritesEveryMatch() {
        ReplaceMatchTransform stage = ReplaceMatchTransform.of(KEYWORD, 1, constantCase());
        assertThat(
            stage.execute(this.ctx, "%{KEYWORD:Mining Speed} and %{KEYWORD:Mining Fortune}."),
            is(equalTo("%{KEYWORD:MINING_SPEED} and %{KEYWORD:MINING_FORTUNE}."))
        );
    }

    @Test
    @DisplayName("An absent group rewrites the whole match")
    void absentGroupRewritesWholeMatch() {
        assertThat(ReplaceMatchTransform.of("[a-z]+", null, upper()).execute(this.ctx, "ab 12 cd"), is(equalTo("AB 12 CD")));
    }

    @Test
    @DisplayName("Group 0 rewrites the whole match")
    void groupZeroRewritesWholeMatch() {
        assertThat(ReplaceMatchTransform.of("[a-z]+", 0, upper()).execute(this.ctx, "ab 12 cd"), is(equalTo("AB 12 CD")));
    }

    @Test
    @DisplayName("An input with no match comes back unchanged")
    void noMatchReturnsInput() {
        assertThat(ReplaceMatchTransform.of(KEYWORD, 1, constantCase()).execute(this.ctx, "no keywords"), is(equalTo("no keywords")));
    }

    @Test
    @DisplayName("An empty input comes back empty")
    void emptyInputReturnsEmpty() {
        assertThat(ReplaceMatchTransform.of("[a-z]*", null, upper()).execute(this.ctx, ""), is(equalTo("")));
    }

    @Test
    @DisplayName("A body that yields null leaves that match unchanged and rewrites the others")
    void nullBodyLeavesMatch() {
        ValueMapTransform lookup = ValueMapTransform.of(Map.of("PINK", "LIGHT_PURPLE"), null, true);
        ReplaceMatchTransform stage = ReplaceMatchTransform.of("\\{([A-Z]+)\\}", 1, List.of(lookup));
        assertThat(stage.execute(this.ctx, "{PINK} {BLACK} {PINK}"), is(equalTo("{LIGHT_PURPLE} {BLACK} {LIGHT_PURPLE}")));
    }

    @Test
    @DisplayName("A group that did not participate leaves that match unchanged")
    void nonParticipatingGroupLeavesMatch() {
        assertThat(ReplaceMatchTransform.of("(a)|b", 1, upper()).execute(this.ctx, "ab"), is(equalTo("Ab")));
    }

    @Test
    @DisplayName("A group overlapping a part already replaced leaves its match unchanged")
    void overlappingGroupLeavesMatch() {
        assertThat(ReplaceMatchTransform.of("(?=(\\w\\w))", 1, upper()).execute(this.ctx, "abc"), is(equalTo("ABc")));
    }

    @Test
    @DisplayName("Only the group is replaced; the rest of the match is kept")
    void restOfMatchIsKept() {
        assertThat(ReplaceMatchTransform.of("key=(\\w+);", 1, upper()).execute(this.ctx, "key=abc;"), is(equalTo("key=ABC;")));
    }

    @Test
    @DisplayName("The body's result is inserted literally, with no group references")
    void bodyResultIsLiteral() {
        ReplaceMatchTransform stage = ReplaceMatchTransform.of("(x)", 1, List.of(AppendTransform.of("$1\\")));
        assertThat(stage.execute(this.ctx, "x"), is(equalTo("x$1\\")));
    }

    @Test
    @DisplayName("of refuses a group past the pattern's group count")
    void ofRefusesGroupPastCount() {
        assertThrows(IllegalArgumentException.class, () -> ReplaceMatchTransform.of("(a)", 2, upper()));
    }

    @Test
    @DisplayName("of refuses a negative group")
    void ofRefusesNegativeGroup() {
        assertThrows(IllegalArgumentException.class, () -> ReplaceMatchTransform.of("(a)", -1, upper()));
    }

    @Test
    @DisplayName("of refuses a pattern that does not compile")
    void ofRefusesBadPattern() {
        assertThrows(IllegalArgumentException.class, () -> ReplaceMatchTransform.of("(a", null, upper()));
    }

    @Test
    @DisplayName("of refuses a body that does not produce a STRING")
    void ofRefusesMistypedBody() {
        assertThrows(IllegalArgumentException.class, () -> ReplaceMatchTransform.of("\\d+", null, List.of(ParseIntTransform.of())));
    }

    @Test
    @DisplayName("of refuses an empty body")
    void ofRefusesEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> ReplaceMatchTransform.of("\\d+", null, List.of()));
    }

    @Test
    @DisplayName("The wire form omits an unset group")
    void wireFormOmitsUnsetGroup() {
        String json = PipelineGson.toJson(pipeline("ab", ReplaceMatchTransform.of("[a-z]+", null, upper())));
        assertThat(json, not(containsString("\"group\"")));
    }

    @Test
    @DisplayName("A pipeline with a group round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pipeline("%{KEYWORD:Heart of the Mountain}", ReplaceMatchTransform.of(KEYWORD, 1, constantCase())));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline with a group round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<String> original = pipeline("%{KEYWORD:Heart of the Mountain}", ReplaceMatchTransform.of(KEYWORD, 1, constantCase()));
        assertThat(PipelineGson.fromJson(PipelineGson.toJson(original)).execute(), is(equalTo(original.execute())));
    }

    @Test
    @DisplayName("A pipeline without a group round-trips to the same JSON")
    void wireRoundTripWithoutGroupIsStable() {
        String first = PipelineGson.toJson(pipeline("ab 12", ReplaceMatchTransform.of("[a-z]+", null, upper())));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A wire file whose body does not produce a STRING fails to load")
    void wireRefusesMistypedBody() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a1\"},"
            + "{\"kind\":\"TRANSFORM_REPLACE_MATCH\",\"regex\":\"\\\\d+\",\"body\":[{\"kind\":\"TRANSFORM_PARSE_INT\"}]}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid replaceMatch body"));
    }

    @Test
    @DisplayName("A wire file whose group is past the pattern's group count fails to load")
    void wireRefusesGroupPastCount() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TRANSFORM_REPLACE_MATCH\",\"regex\":\"a\",\"group\":1,\"body\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}]}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("ReplaceMatchTransform group '1'"));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

}
