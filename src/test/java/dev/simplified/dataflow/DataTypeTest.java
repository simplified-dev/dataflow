package dev.simplified.dataflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class DataTypeTest {

    @Test
    @DisplayName("Two basic types are equal when their labels match, even when their Java type matches")
    void basicTypeIdentityIsByLabel() {
        DataType<String> a = new DataType.Basic<>(String.class, "RAW_HTML");
        DataType<String> b = new DataType.Basic<>(String.class, "RAW_HTML");
        assertThat(a, is(equalTo(b)));
    }

    @Test
    @DisplayName("Distinct labels with the same Java type are not equal")
    void distinctLabelsAreNotEqual() {
        assertThat(DataTypes.RAW_HTML, is(not(equalTo(DataTypes.RAW_XML))));
        assertThat(DataTypes.RAW_HTML, is(not(equalTo(DataTypes.STRING))));
    }

    @Test
    @DisplayName("List type label includes the element type label")
    void listTypeLabel() {
        DataType<?> listOfString = DataType.list(DataTypes.STRING);
        assertThat(listOfString.label(), is(equalTo("List<STRING>")));
    }

    @Test
    @DisplayName("Set type label includes the element type label")
    void setTypeLabel() {
        DataType<?> setOfInt = DataType.set(DataTypes.INT);
        assertThat(setOfInt.label(), is(equalTo("Set<INT>")));
    }

    @Test
    @DisplayName("Two list types over the same element are equal")
    void listTypeEquality() {
        assertThat(DataType.list(DataTypes.INT), is(equalTo(DataType.list(DataTypes.INT))));
        assertThat(DataType.list(DataTypes.INT), is(not(equalTo(DataType.list(DataTypes.STRING)))));
    }

    @Nested
    @DisplayName("isAssignableTo")
    class Assignability {

        @Test
        @DisplayName("holds for a basic type and itself")
        void basicToItself() {
            assertThat(DataTypes.STRING.isAssignableTo(DataTypes.STRING), is(true));
        }

        @Test
        @DisplayName("holds for a list type and an equal list type")
        void listToEqualList() {
            assertThat(DataType.list(DataTypes.INT).isAssignableTo(DataType.list(DataTypes.INT)), is(true));
        }

        @Test
        @DisplayName("widens JSON_OBJECT to JSON_ELEMENT")
        void jsonObjectToJsonElement() {
            assertThat(DataTypes.JSON_OBJECT.isAssignableTo(DataTypes.JSON_ELEMENT), is(true));
        }

        @Test
        @DisplayName("widens JSON_ARRAY to JSON_ELEMENT")
        void jsonArrayToJsonElement() {
            assertThat(DataTypes.JSON_ARRAY.isAssignableTo(DataTypes.JSON_ELEMENT), is(true));
        }

        @Test
        @DisplayName("does not narrow JSON_ELEMENT to JSON_OBJECT")
        void jsonElementNotToJsonObject() {
            assertThat(DataTypes.JSON_ELEMENT.isAssignableTo(DataTypes.JSON_OBJECT), is(false));
        }

        @Test
        @DisplayName("does not carry JSON_OBJECT across to JSON_ARRAY")
        void jsonObjectNotToJsonArray() {
            assertThat(DataTypes.JSON_OBJECT.isAssignableTo(DataTypes.JSON_ARRAY), is(false));
        }

        @Test
        @DisplayName("does not widen between labels over the same Java type")
        void sameJavaTypeOtherLabel() {
            assertThat(DataTypes.RAW_HTML.isAssignableTo(DataTypes.STRING), is(false));
        }

        @Test
        @DisplayName("does not widen INT to LONG")
        void intNotToLong() {
            assertThat(DataTypes.INT.isAssignableTo(DataTypes.LONG), is(false));
        }

        @Test
        @DisplayName("does not widen FLOAT to DOUBLE")
        void floatNotToDouble() {
            assertThat(DataTypes.FLOAT.isAssignableTo(DataTypes.DOUBLE), is(false));
        }

        @Test
        @DisplayName("widens a list to a list of a type its element widens to")
        void listWidensByElement() {
            assertThat(DataType.list(DataTypes.JSON_OBJECT).isAssignableTo(DataType.list(DataTypes.JSON_ELEMENT)), is(true));
        }

        @Test
        @DisplayName("widens a set to a set of a type its element widens to")
        void setWidensByElement() {
            assertThat(DataType.set(DataTypes.JSON_ARRAY).isAssignableTo(DataType.set(DataTypes.JSON_ELEMENT)), is(true));
        }

        @Test
        @DisplayName("widens through nested lists")
        void nestedListWidens() {
            DataType<?> rows = DataType.list(DataType.list(DataTypes.JSON_OBJECT));
            assertThat(rows.isAssignableTo(DataType.list(DataType.list(DataTypes.JSON_ELEMENT))), is(true));
        }

        @Test
        @DisplayName("does not narrow a list's element")
        void listDoesNotNarrow() {
            assertThat(DataType.list(DataTypes.JSON_ELEMENT).isAssignableTo(DataType.list(DataTypes.JSON_OBJECT)), is(false));
        }

        @Test
        @DisplayName("does not carry a list across to a set")
        void listNotToSet() {
            assertThat(DataType.list(DataTypes.JSON_OBJECT).isAssignableTo(DataType.set(DataTypes.JSON_ELEMENT)), is(false));
        }

        @Test
        @DisplayName("does not carry a set across to a list")
        void setNotToList() {
            assertThat(DataType.set(DataTypes.STRING).isAssignableTo(DataType.list(DataTypes.STRING)), is(false));
        }

        @Test
        @DisplayName("does not carry JSON_ARRAY across to a list")
        void jsonArrayNotToList() {
            assertThat(DataTypes.JSON_ARRAY.isAssignableTo(DataType.list(DataTypes.JSON_ELEMENT)), is(false));
        }

    }

}
