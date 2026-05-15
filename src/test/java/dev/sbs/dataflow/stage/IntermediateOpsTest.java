package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.filter.list.DropWhileFilter;
import dev.sbs.dataflow.stage.filter.list.TakeWhileFilter;
import dev.sbs.dataflow.stage.transform.list.SortTransform;
import dev.sbs.dataflow.stage.transform.primitive.ParseBooleanTransform;
import dev.sbs.dataflow.stage.transform.primitive.PeekTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntermediateOpsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Sort ascending orders INT smallest-first")
    void sortIntAscending() {
        SortTransform<Integer> sort = SortTransform.of(DataTypes.INT, true);
        assertThat(sort.execute(this.ctx, List.of(3, 1, 4, 1, 5, 9, 2, 6)), contains(1, 1, 2, 3, 4, 5, 6, 9));
    }

    @Test
    @DisplayName("Sort descending orders STRING largest-first")
    void sortStringDescending() {
        SortTransform<String> sort = SortTransform.of(DataTypes.STRING, false);
        assertThat(sort.execute(this.ctx, List.of("b", "a", "c")), contains("c", "b", "a"));
    }

    @Test
    @DisplayName("Sort rejects unsupported element types")
    void sortRejectsUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> SortTransform.of(DataTypes.BOOLEAN, true));
    }

    @Test
    @DisplayName("Peek returns the input unchanged")
    void peekIsIdentity() {
        PeekTransform<String> peek = PeekTransform.of(DataTypes.STRING, "test");
        assertThat(peek.execute(this.ctx, "hello"), is(equalTo("hello")));
    }

    @Test
    @DisplayName("TakeWhile keeps the leading prefix that matches the predicate")
    void takeWhileKeepsPrefix() {
        TakeWhileFilter<String> tw = TakeWhileFilter.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(tw.execute(this.ctx, List.of("true", "true", "false", "true")), contains("true", "true"));
    }

    @Test
    @DisplayName("TakeWhile on empty list yields empty")
    void takeWhileEmpty() {
        TakeWhileFilter<String> tw = TakeWhileFilter.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(tw.execute(this.ctx, List.of()), is(List.of()));
    }

    @Test
    @DisplayName("DropWhile discards the leading prefix that matches the predicate")
    void dropWhileDiscardsPrefix() {
        DropWhileFilter<String> dw = DropWhileFilter.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(dw.execute(this.ctx, List.of("true", "true", "false", "true")), contains("false", "true"));
    }

    @Test
    @DisplayName("DropWhile keeps the full list when the first element fails the predicate")
    void dropWhileNoDropping() {
        DropWhileFilter<String> dw = DropWhileFilter.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(dw.execute(this.ctx, List.of("false", "true", "true")), contains("false", "true", "true"));
    }

    @Test
    @DisplayName("TakeWhile / DropWhile reject empty bodies")
    void emptyBodiesRejected() {
        assertThrows(IllegalArgumentException.class, () -> TakeWhileFilter.of(DataTypes.STRING, List.of()));
        assertThrows(IllegalArgumentException.class, () -> DropWhileFilter.of(DataTypes.STRING, List.of()));
    }

}
