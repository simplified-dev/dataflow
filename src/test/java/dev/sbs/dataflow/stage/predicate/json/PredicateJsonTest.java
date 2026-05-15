package dev.sbs.dataflow.stage.predicate.json;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PredicateJsonTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Has field")
    void hasField() {
        JsonObject obj = JsonParser.parseString("{\"rare\":true,\"name\":\"foo\"}").getAsJsonObject();
        assertThat(JsonHasFieldPredicate.of("rare").execute(this.ctx, obj), is(true));
        assertThat(JsonHasFieldPredicate.of("missing").execute(this.ctx, obj), is(false));
        assertThat(JsonHasFieldPredicate.of("rare").execute(this.ctx, null), is(false));
    }

    @Test
    @DisplayName("Field equals primitive value")
    void fieldEquals() {
        JsonObject obj = JsonParser.parseString("{\"rare\":\"true\",\"count\":5}").getAsJsonObject();
        assertThat(JsonFieldEqualsPredicate.of("rare", "true").execute(this.ctx, obj), is(true));
        assertThat(JsonFieldEqualsPredicate.of("rare", "false").execute(this.ctx, obj), is(false));
        assertThat(JsonFieldEqualsPredicate.of("count", "5").execute(this.ctx, obj), is(true));
        assertThat(JsonFieldEqualsPredicate.of("missing", "anything").execute(this.ctx, obj), is(false));
    }

}
