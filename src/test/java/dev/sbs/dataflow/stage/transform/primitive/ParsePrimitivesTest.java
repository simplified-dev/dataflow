package dev.sbs.dataflow.stage.transform.primitive;

import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class ParsePrimitivesTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("ParseLong handles valid input and returns null on garbage")
    void parseLong() {
        assertThat(ParseLongTransform.of().execute(this.ctx, " 9999999999 "), is(equalTo(9_999_999_999L)));
        assertThat(ParseLongTransform.of().execute(this.ctx, "nope"), is(nullValue()));
        assertThat(ParseLongTransform.of().execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("ParseFloat handles valid input and returns null on garbage")
    void parseFloat() {
        assertThat(ParseFloatTransform.of().execute(this.ctx, "3.14"), is(equalTo(3.14f)));
        assertThat(ParseFloatTransform.of().execute(this.ctx, "x"), is(nullValue()));
    }

    @Test
    @DisplayName("ParseBoolean accepts true/false/1/0/yes/no, otherwise null")
    void parseBoolean() {
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "true"), is(true));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "TRUE"), is(true));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "1"), is(true));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "yes"), is(true));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "false"), is(false));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "0"), is(false));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "no"), is(false));
        assertThat(ParseBooleanTransform.of().execute(this.ctx, "maybe"), is(nullValue()));
    }

}
