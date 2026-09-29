package dev.simplified.dataflow.stage;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.filter.list.WhereFilter;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.predicate.common.ComparePredicate;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.list.SizeTransform;
import dev.simplified.dataflow.stage.transform.primitive.ExpectTransform;
import dev.simplified.dataflow.stage.transform.string.TrimTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Acceptance smoke tests for the new {@link StageRegistry}.
 */
class StageRegistryTest {

    /**
     * Stages that test a condition over each value - keep it, compare two of its values, or fail
     * the run when it breaks an expectation - each with the category it declares.
     */
    private static final @NotNull List<ConditionStage> CONDITION_STAGES = List.of(
        new ConditionStage("FILTER_WHERE", WhereFilter.class, StageSpec.Category.FILTER_LIST),
        new ConditionStage("PREDICATE_COMPARE", ComparePredicate.class, StageSpec.Category.PREDICATE_COMMON),
        new ConditionStage("TRANSFORM_EXPECT", ExpectTransform.class, StageSpec.Category.TRANSFORM_PRIMITIVE)
    );

    @Test
    @DisplayName("Registry discovers at least the 134 concrete stage classes from the brief")
    void registrySizeMatches() {
        assertThat(StageRegistry.allOrdered().size(), is(greaterThanOrEqualTo(134)));
    }

    @Test
    @DisplayName("byId('TRANSFORM_LIST_LENGTH') returns SizeTransform")
    void byIdResolvesKnown() {
        assertThat(StageRegistry.byId("TRANSFORM_LIST_LENGTH"), is(sameInstance(SizeTransform.class)));
    }

    @Test
    @DisplayName("byId on an unknown id throws IllegalArgumentException naming the missing id")
    void byIdRejectsUnknown() {
        try {
            StageRegistry.byId("NOT_A_REAL_ID");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("NOT_A_REAL_ID"));
            return;
        }
        throw new AssertionError("Expected IllegalArgumentException for unknown id");
    }

    @Test
    @DisplayName("Wire format byte-identity: two consecutive serialisations yield the same JSON")
    void wireFormatStable() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralSource.of(DataTypes.STRING, "  hi  "))
            .stage(TrimTransform.of())
            .build();
        String first = PipelineGson.toJson(pipeline);
        String second = PipelineGson.toJson(pipeline);
        assertThat(second, is(equalTo(first)));
        // Existing id strings remain stable on the wire.
        assertThat(first, containsString("\"kind\":\"SOURCE_LITERAL\""));
        assertThat(first, containsString("\"kind\":\"TRANSFORM_TRIM\""));
    }

    @TestFactory
    Stream<DynamicTest> conditionStageResolvesById() {
        return CONDITION_STAGES.stream().map(stage -> DynamicTest.dynamicTest(
            stage.id(),
            () -> assertThat(StageRegistry.byId(stage.id()), is(sameInstance(stage.type())))
        ));
    }

    @TestFactory
    Stream<DynamicTest> conditionStageDeclaresItsCategory() {
        return CONDITION_STAGES.stream().map(stage -> DynamicTest.dynamicTest(
            stage.id(),
            () -> assertThat(categoryOf(StageRegistry.byId(stage.id())), is(equalTo(stage.category())))
        ));
    }

    @TestFactory
    Stream<DynamicTest> conditionStageSitsInItsCategoryPackage() {
        return CONDITION_STAGES.stream().map(stage -> DynamicTest.dynamicTest(
            stage.id(),
            () -> assertThat(
                stage.type().getPackageName(),
                is(equalTo("dev.simplified.dataflow.stage." + stage.category().name().toLowerCase().replace('_', '.')))
            )
        ));
    }

    @TestFactory
    Stream<DynamicTest> conditionStageIsOrderedWithinItsCategory() {
        return CONDITION_STAGES.stream().map(stage -> DynamicTest.dynamicTest(
            stage.id(),
            () -> {
                List<Class<? extends Stage<?, ?>>> ordered = StageRegistry.allOrdered();
                int index = ordered.indexOf(stage.type());
                List<String> misplaced = IntStream.range(0, ordered.size())
                    .filter(i -> {
                        int order = categoryOf(ordered.get(i)).compareTo(stage.category());
                        return i < index ? order > 0 : i > index && order < 0;
                    })
                    .mapToObj(i -> ordered.get(i).getSimpleName())
                    .toList();

                assertThat(index, is(greaterThanOrEqualTo(0)));
                assertThat(misplaced, is(empty()));
            }
        ));
    }

    private static @NotNull StageSpec.Category categoryOf(@NotNull Class<?> type) {
        return type.getAnnotation(StageSpec.class).category();
    }

    /**
     * One registered stage and the category it declares.
     *
     * @param id the wire id the stage is registered under
     * @param type the stage class the id resolves to
     * @param category the palette category the class declares
     */
    private record ConditionStage(@NotNull String id, @NotNull Class<?> type, @NotNull StageSpec.Category category) { }

}
