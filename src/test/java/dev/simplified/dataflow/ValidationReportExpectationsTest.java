package dev.simplified.dataflow;

import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.filter.list.WhereFilter;
import dev.simplified.dataflow.stage.fixture.AppendOperandTransform;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.predicate.common.AndPredicate;
import dev.simplified.dataflow.stage.predicate.common.NotNullPredicate;
import dev.simplified.dataflow.stage.predicate.string.NonEmptyPredicate;
import dev.simplified.dataflow.stage.predicate.string.StartsWithPredicate;
import dev.simplified.dataflow.stage.source.LiteralListSource;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.MapCollect;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import dev.simplified.dataflow.stage.transform.primitive.CoalesceTransform;
import dev.simplified.dataflow.stage.transform.primitive.ExpectTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Covers the expectations {@link DataPipeline#validate()} lists in a {@link ValidationReport}:
 * one per {@link ExpectTransform}, wherever it sits, each with the path of the stage that states
 * it.
 */
class ValidationReportExpectationsTest {

    /**
     * Stage carrying {@link StageSpec} without a canonical factory, so its metadata cannot be
     * derived. It sits outside the registry's package and never loads from the wire.
     */
    @StageSpec(id = "TEST_UNDERIVABLE", displayName = "Underivable", description = "STRING -> STRING", category = StageSpec.Category.TRANSFORM_PRIMITIVE)
    private static final class UnderivableStage implements TransformStage<String, String> {

        /** {@inheritDoc} */
        @Override
        public @NotNull DataType<String> inputType() {
            return DataTypes.STRING;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull DataType<String> outputType() {
            return DataTypes.STRING;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull String summary() {
            return "Underivable";
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
            return input;
        }

    }

    private static @NotNull ExpectTransform<String> nonEmpty(@NotNull String expectation) {
        return ExpectTransform.of(DataTypes.STRING, expectation, List.of(NonEmptyPredicate.of()));
    }

    private static @NotNull ValidationReport.Expectation expectation(int stageIndex, @NotNull String path, @NotNull String text) {
        return new ValidationReport.Expectation(stageIndex, path, text, DataTypes.STRING);
    }

    private static @NotNull DataPipeline<String> topLevel() {
        return DataPipeline.builder()
            .source(LiteralSource.text("item_1"))
            .stage(nonEmpty("the id is present"))
            .stage(UpperCaseTransform.of())
            .build();
    }

    private static @NotNull DataPipeline<?> inMapBody() {
        return DataPipeline.builder()
            .source(LiteralListSource.strings("a", "b"))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.STRING, List.of(UpperCaseTransform.of(), nonEmpty("each name is present"))))
            .build();
    }

    @Test
    @DisplayName("A top-level expect stage is listed with its index, path, text and input type")
    void topLevelListed() {
        assertThat(topLevel().validate().expectations(), contains(expectation(1, "#1", "the id is present")));
    }

    @Test
    @DisplayName("A pipeline with expectations and no issues is valid")
    void expectationsKeepPipelineValid() {
        assertThat(topLevel().validate().isValid(), is(true));
    }

    @Test
    @DisplayName("Expectations are not issues")
    void expectationsAreNotIssues() {
        assertThat(topLevel().validate().issues(), is(empty()));
    }

    @Test
    @DisplayName("Builder.validate lists the expectations of the stages staged so far")
    void builderValidateLists() {
        ValidationReport report = DataPipeline.builder()
            .source(LiteralSource.text("item_1"))
            .stage(nonEmpty("the id is present"))
            .validate();
        assertThat(report.expectations(), contains(expectation(1, "#1", "the id is present")));
    }

    @Test
    @DisplayName("An expect stage in a body is listed under its enclosing stage's index with the body path")
    void bodyListed() {
        assertThat(inMapBody().validate().expectations(), contains(expectation(1, "#1.body[1]", "each name is present")));
    }

    @Test
    @DisplayName("A pipeline with a nested expectation is valid")
    void nestedExpectationKeepsPipelineValid() {
        assertThat(inMapBody().validate().isValid(), is(true));
    }

    @Test
    @DisplayName("An expect stage inside an expect stage's body is listed after it")
    void expectInsideExpect() {
        ExpectTransform<String> outer = ExpectTransform.of(
            DataTypes.STRING, "the id is an item id", List.of(nonEmpty("the id is present"), StartsWithPredicate.of("item_"))
        );
        DataPipeline<?> pipeline = DataPipeline.builder().source(LiteralSource.text("item_1")).stage(outer).build();
        assertThat(pipeline.validate().expectations(), contains(
            expectation(1, "#1", "the id is an item id"),
            expectation(1, "#1.body[0]", "the id is present")
        ));
    }

    @Test
    @DisplayName("An expect stage in a named body is listed with the slot, branch and index")
    void namedBodyListed() {
        MapCollect<String> collect = MapCollect.of(DataTypes.STRING, NamedChains.of(Map.of(
            "length", List.of(nonEmpty("the name is present"), LengthTransform.of())
        )));
        DataPipeline<?> pipeline = DataPipeline.builder().source(LiteralSource.text("item_1")).stage(collect).build();
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1.outputs.length[0]", "the name is present")));
    }

    @Test
    @DisplayName("An expect stage in a typed body is listed with the slot, branch, chain and index")
    void typedBodyListed() {
        ObjectBuildTransform<String> build = ObjectBuildTransform.over(DataTypes.STRING)
            .output("id", DataTypes.STRING, chain -> chain.stage(UpperCaseTransform.of()).stage(nonEmpty("the id is present")))
            .build();
        DataPipeline<?> pipeline = DataPipeline.builder().source(LiteralSource.text("item_1")).stage(build).build();
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1.outputs.id.chain[1]", "the id is present")));
    }

    @Test
    @DisplayName("An expect stage in a pipeline operand is listed with the operand slot and index")
    void operandListed() {
        DataPipeline<String> suffix = DataPipeline.builder()
            .source(LiteralSource.text("_x"))
            .stage(nonEmpty("the suffix is present"))
            .build();
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("item"))
            .stage(AppendOperandTransform.of(suffix))
            .build();
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1.suffix[1]", "the suffix is present")));
    }

    @Test
    @DisplayName("An expect stage several levels deep is listed with every level in its path")
    void deepNestingListed() {
        AndPredicate<String> and = AndPredicate.of(DataTypes.STRING, Map.of(
            "item", List.<Stage<?, ?>>of(nonEmpty("the id is present"), StartsWithPredicate.of("item_"))
        ));
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings("item_1", "mob_2"))
            .stage(WhereFilter.of(DataTypes.STRING, List.of(and)))
            .build();
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1.body[0].bodies.item[0]", "the id is present")));
    }

    @Test
    @DisplayName("An absent optional body is skipped")
    void absentOptionalBodySkipped() {
        CoalesceTransform<String, String> coalesce = CoalesceTransform.of(
            DataTypes.STRING, DataTypes.STRING, List.of(nonEmpty("the name is present")), null, "unknown", null
        );
        DataPipeline<?> pipeline = DataPipeline.builder().source(LiteralSource.text("item_1")).stage(coalesce).build();
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1.body[0]", "the name is present")));
    }

    @Test
    @DisplayName("Expectations are listed in walk order, a stage before its bodies and bodies before later stages")
    void walkOrder() {
        WhereFilter<String> named = WhereFilter.of(DataTypes.STRING, List.of(nonEmpty("each name is present"), NonEmptyPredicate.of()));
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralListSource.strings("a", "b"))
            .stage(ExpectTransform.of(DataType.list(DataTypes.STRING), "the list is present", List.of(
                named, NotNullPredicate.of(DataType.list(DataTypes.STRING))
            )))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.STRING, List.of(nonEmpty("each name is still present"))))
            .build();
        assertThat(pipeline.validate().expectations().stream().map(ValidationReport.Expectation::path).toList(), contains(
            "#1", "#1.body[0].body[0]", "#2.body[0]"
        ));
    }

    @Test
    @DisplayName("An invalid pipeline still lists its expectations")
    void invalidPipelineLists() {
        DataPipeline<?> pipeline = DataPipeline.unchecked(
            List.<Stage<?, ?>>of(LiteralSource.text("item_1"), nonEmpty("the id is present"), LengthTransform.of(), UpperCaseTransform.of()),
            DataTypes.STRING
        );
        assertThat(pipeline.validate().expectations(), contains(expectation(1, "#1", "the id is present")));
    }

    @Test
    @DisplayName("validate with an expected type keeps the expectations when it adds an issue")
    void operandCheckKeepsExpectations() {
        assertThat(topLevel().validate(DataTypes.INT).expectations(), contains(expectation(1, "#1", "the id is present")));
    }

    @Test
    @DisplayName("A pipeline without an expect stage lists no expectations")
    void noExpectStage() {
        DataPipeline<?> pipeline = DataPipeline.builder().source(LiteralSource.text("x")).stage(UpperCaseTransform.of()).build();
        assertThat(pipeline.validate().expectations(), is(empty()));
    }

    @Test
    @DisplayName("The empty pipeline lists no expectations")
    void emptyPipeline() {
        assertThat(DataPipeline.empty().validate().expectations(), is(empty()));
    }

    @Test
    @DisplayName("A pipeline read from the wire lists a nested expectation with its path")
    void wirePipelineLists() {
        String json = "[{\"kind\":\"SOURCE_LITERAL_LIST\",\"elementType\":\"STRING\",\"value\":\"[\\\"item_1\\\"]\"},"
            + "{\"kind\":\"TRANSFORM_MAP\",\"elementInputType\":\"STRING\",\"elementOutputType\":\"STRING\",\"body\":["
            + "{\"kind\":\"TRANSFORM_EXPECT\",\"inputType\":\"STRING\",\"expectation\":\"each id is an item id\","
            + "\"body\":[{\"kind\":\"PREDICATE_STRING_STARTS_WITH\",\"prefix\":\"item_\"}]}]}]";
        assertThat(PipelineGson.fromJson(json).validate().expectations(), contains(expectation(1, "#1.body[0]", "each id is an item id")));
    }

    @Test
    @DisplayName("A report built from issues alone carries no expectations")
    void issuesOnlyReport() {
        assertThat(new ValidationReport(List.of()).expectations(), is(empty()));
    }

    @Test
    @DisplayName("A stage whose metadata cannot be derived nests nothing and does not fail the validation")
    void underivableStageNestsNothing() {
        DataPipeline<?> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("item_1"))
            .stage(new UnderivableStage())
            .stage(nonEmpty("the id is present"))
            .build();
        assertThat(pipeline.validate().expectations(), contains(expectation(2, "#2", "the id is present")));
    }

}
