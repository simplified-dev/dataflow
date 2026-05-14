package dev.sbs.dataflow.stage.terminal;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.primitive.ParseBooleanTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollectStreamOpsTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("Count returns the list size")
    void count() {
        assertThat(CountCollect.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "c")), is(3));
        assertThat(CountCollect.of(DataTypes.STRING).execute(this.ctx, List.of()), is(0));
        assertThat(CountCollect.of(DataTypes.STRING).execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Min picks the smallest element by natural ordering")
    void min() {
        assertThat(MinCollect.of(DataTypes.INT).execute(this.ctx, List.of(3, 1, 4, 1, 5)), is(1));
        assertThat(MinCollect.of(DataTypes.STRING).execute(this.ctx, List.of("z", "a", "m")), is(equalTo("a")));
        assertThat(MinCollect.of(DataTypes.INT).execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("Max picks the largest element by natural ordering")
    void max() {
        assertThat(MaxCollect.of(DataTypes.INT).execute(this.ctx, List.of(3, 1, 4, 1, 5)), is(5));
        assertThat(MaxCollect.of(DataTypes.STRING).execute(this.ctx, List.of("z", "a", "m")), is(equalTo("z")));
    }

    @Test
    @DisplayName("Min/Max reject unsupported element types")
    void minMaxRejectUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> MinCollect.of(DataTypes.BOOLEAN));
        assertThrows(IllegalArgumentException.class, () -> MaxCollect.of(DataTypes.DOM_NODE));
    }

    @Test
    @DisplayName("Sum returns the total for INT / LONG / DOUBLE")
    void sum() {
        assertThat(SumIntCollect.of().execute(this.ctx, List.of(1, 2, 3)), is(6));
        assertThat(SumLongCollect.of().execute(this.ctx, List.of(1L, 2L, 3L)), is(6L));
        assertThat(SumDoubleCollect.of().execute(this.ctx, List.of(1.5, 2.5)), is(4.0));
        assertThat(SumIntCollect.of().execute(this.ctx, List.of()), is(0));
    }

    @Test
    @DisplayName("Average returns the arithmetic mean as DOUBLE; null on empty")
    void average() {
        assertThat(AverageIntCollect.of().execute(this.ctx, List.of(1, 2, 3, 4)), is(closeTo(2.5, 1e-9)));
        assertThat(AverageLongCollect.of().execute(this.ctx, List.of(10L, 20L)), is(closeTo(15.0, 1e-9)));
        assertThat(AverageDoubleCollect.of().execute(this.ctx, List.of(1.0, 2.0, 3.0)), is(closeTo(2.0, 1e-9)));
        assertThat(AverageIntCollect.of().execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("FindFirst returns the first element whose predicate body yields true")
    void findFirst() {
        FindFirstCollect<String> stage = FindFirstCollect.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(stage.execute(this.ctx, List.of("false", "false", "true", "true")), is(equalTo("true")));
        assertThat(stage.execute(this.ctx, List.of("false", "false")), is(nullValue()));
        assertThat(stage.execute(this.ctx, List.of()), is(nullValue()));
    }

    @Test
    @DisplayName("AnyMatch returns true when at least one element matches; false otherwise")
    void anyMatch() {
        AnyMatchCollect<String> stage = AnyMatchCollect.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(stage.execute(this.ctx, List.of("false", "true")), is(true));
        assertThat(stage.execute(this.ctx, List.of("false", "false")), is(false));
        assertThat(stage.execute(this.ctx, List.of()), is(false));
    }

    @Test
    @DisplayName("AllMatch returns true only when every element matches; empty list is true")
    void allMatch() {
        AllMatchCollect<String> stage = AllMatchCollect.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(stage.execute(this.ctx, List.of("true", "true", "true")), is(true));
        assertThat(stage.execute(this.ctx, List.of("true", "false")), is(false));
        assertThat(stage.execute(this.ctx, List.of()), is(true));
    }

    @Test
    @DisplayName("NoneMatch returns true only when no element matches; empty list is true")
    void noneMatch() {
        NoneMatchCollect<String> stage = NoneMatchCollect.of(
            DataTypes.STRING,
            List.of(ParseBooleanTransform.of())
        );
        assertThat(stage.execute(this.ctx, List.of("false", "false")), is(true));
        assertThat(stage.execute(this.ctx, List.of("false", "true")), is(false));
        assertThat(stage.execute(this.ctx, List.of()), is(true));
    }

    @Test
    @DisplayName("Predicate-body collectors reject bodies whose output type is not BOOLEAN")
    void predicateBodiesRejectNonBooleanOutput() {
        assertThrows(IllegalArgumentException.class, () -> AnyMatchCollect.of(DataTypes.STRING, List.of()));
        assertThrows(IllegalArgumentException.class, () -> FindFirstCollect.of(DataTypes.STRING, List.of()));
    }

}
