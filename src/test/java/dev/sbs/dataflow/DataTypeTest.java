package dev.sbs.dataflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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

}
