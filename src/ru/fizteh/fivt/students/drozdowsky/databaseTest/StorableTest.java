package ru.fizteh.fivt.students.drozdowsky.databaseTest;

import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;

import java.util.Arrays;
import java.util.List;

public class StorableTest {
    public static Storable stor;
    public static List<Class<?>> types;

    @Before
    public void setUp() throws Exception {
        Class<?>[] typesAsArray = {Integer.class, Long.class, Byte.class, Float.class, Double.class,
                    Boolean.class, String.class};
        Object[] valuesAsArray = {1, 1L, (byte) 1, (float) 1., (double) 1., true, "asd"};
        types = Arrays.asList(typesAsArray);
        stor = new Storable(types, Arrays.asList(valuesAsArray));
    }


    @Test(expected = IllegalArgumentException.class)
    public void initNullShouldFail() {
        stor = new Storable(null);
    }

    @Test(expected = ColumnFormatException.class)
    public void putNonvalidValueShouldFail() {
        stor.setColumnAt(0, "asd");
    }

    @Test(expected = ColumnFormatException.class)
    public void getMismatchedFieldShouldFail() {
        stor.getStringAt(0);
    }

    @Test
    public void testGetIntAt() throws Exception {
        stor.getIntAt(0);
    }

    @Test
    public void testGetLongAt() throws Exception {
        stor.getLongAt(1);
    }

    @Test
    public void testGetByteAt() throws Exception {
        stor.getByteAt(2);
    }

    @Test
    public void testGetFloatAt() throws Exception {
        stor.getFloatAt(3);
    }

    @Test
    public void testGetDoubleAt() throws Exception {
        stor.getDoubleAt(4);
    }

    @Test
    public void testGetBooleanAt() throws Exception {
        stor.getBooleanAt(5);
    }

    @Test
    public void testGetStringAt() throws Exception {
        stor.getStringAt(6);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getIntShouldFailIOOB() {
        stor.getIntAt(50);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getLongShouldFailIOOB() {
        stor.getLongAt(-10);
    }
}
