package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.stage.meta.StageMetadata;
import dev.sbs.dataflow.stage.meta.StageReflection;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.chain.Chain;
import dev.sbs.dataflow.stage.filter.list.TakeWhileFilter;
import dev.sbs.dataflow.stage.filter.string.StringContainsFilter;
import dev.sbs.dataflow.stage.predicate.numeric.IntGreaterThanPredicate;
import dev.sbs.dataflow.stage.transform.list.MapTransform;
import dev.sbs.dataflow.stage.transform.string.StringLengthTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Pilot smoke tests for the reflection-driven {@link StageMetadata#buildConfig}
 * and {@link StageMetadata#fromConfig} round-trip, exercising the three slot patterns:
 * <ul>
 *   <li>simple STRING slot ({@code StringContainsFilter})</li>
 *   <li>SUB_PIPELINE slot with a {@code List<? extends Stage<?, ?>>} factory parameter ({@code TakeWhileFilter})</li>
 *   <li>two DATA_TYPE slots with wire-key overrides plus a SUB_PIPELINE slot ({@code MapTransform})</li>
 * </ul>
 */
class StageReflectionTest {

    @Test
    @DisplayName("StringContainsFilter: buildConfig writes 'needle', fromConfig rebuilds")
    void stringContainsFilterRoundTrips() {
        StringContainsFilter instance = StringContainsFilter.of("foo");
        StageMetadata metadata = StageReflection.of(StringContainsFilter.class);

        StageConfig built = metadata.buildConfig(instance);
        assertThat(built.has("needle"), is(true));
        assertThat(built.getString("needle"), is(equalTo("foo")));

        Stage<?, ?> rebuilt = metadata.fromConfig(built);
        assertThat(rebuilt, is(instanceOf(StringContainsFilter.class)));
        assertThat(((StringContainsFilter) rebuilt).needle(), is(equalTo("foo")));
    }

    @Test
    @DisplayName("TakeWhileFilter: SUB_PIPELINE adapter converts Chain -> List<Stage>")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void takeWhileFilterRoundTrips() {
        TakeWhileFilter<Integer> instance = TakeWhileFilter.of(
            DataTypes.INT,
            List.of(IntGreaterThanPredicate.of(0))
        );
        StageMetadata metadata = StageReflection.of((Class<? extends Stage<?, ?>>) (Class) TakeWhileFilter.class);

        StageConfig built = metadata.buildConfig(instance);
        assertThat(built.has("elementType"), is(true));
        assertThat(built.getDataType("elementType"), is(sameInstance(DataTypes.INT)));
        assertThat(built.has("body"), is(true));
        Chain bodyChain = built.getSubPipeline("body");
        assertThat(bodyChain, is(notNullValue()));
        assertThat(bodyChain.size(), is(equalTo(1)));

        Stage<?, ?> rebuilt = metadata.fromConfig(built);
        assertThat(rebuilt, is(instanceOf(TakeWhileFilter.class)));
        TakeWhileFilter<?> typed = (TakeWhileFilter<?>) rebuilt;
        assertThat(typed.elementType(), is(sameInstance(DataTypes.INT)));
        assertThat(typed.body().size(), is(equalTo(1)));

        // Round-tripped config should carry equivalent slot values
        StageConfig rebuiltCfg = metadata.buildConfig(typed);
        assertThat(rebuiltCfg.getDataType("elementType"), is(sameInstance(built.getDataType("elementType"))));
        assertThat(rebuiltCfg.getSubPipeline("body").size(), is(equalTo(built.getSubPipeline("body").size())));
    }

    @Test
    @DisplayName("MapTransform: wire-key overrides + SUB_PIPELINE adapter")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void mapTransformRoundTrips() {
        MapTransform<String, Integer> instance = MapTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(StringLengthTransform.of())
        );
        StageMetadata metadata = StageReflection.of((Class<? extends Stage<?, ?>>) (Class) MapTransform.class);

        StageConfig built = metadata.buildConfig(instance);
        // wire keys, not java param names
        assertThat(built.has("elementInputType"), is(true));
        assertThat(built.has("elementOutputType"), is(true));
        assertThat(built.has("body"), is(true));
        assertThat(built.getDataType("elementInputType"), is(sameInstance(DataTypes.STRING)));
        assertThat(built.getDataType("elementOutputType"), is(sameInstance(DataTypes.INT)));
        assertThat(built.getSubPipeline("body").size(), is(equalTo(1)));

        Stage<?, ?> rebuilt = metadata.fromConfig(built);
        assertThat(rebuilt, is(instanceOf(MapTransform.class)));

        StageConfig rebuiltCfg = metadata.buildConfig(rebuilt);
        assertThat(rebuiltCfg.getDataType("elementInputType"), is(sameInstance(built.getDataType("elementInputType"))));
        assertThat(rebuiltCfg.getDataType("elementOutputType"), is(sameInstance(built.getDataType("elementOutputType"))));
        assertThat(rebuiltCfg.getSubPipeline("body").size(), is(equalTo(built.getSubPipeline("body").size())));
    }

}
