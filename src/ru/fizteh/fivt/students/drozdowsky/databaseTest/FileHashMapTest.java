package ru.fizteh.fivt.students.drozdowsky.databaseTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.students.drozdowsky.database.FileHashMap;
import ru.fizteh.fivt.students.drozdowsky.database.MultiFileHashMap;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FileHashMapTest {
    static FileHashMap table;
    static File databaseDir;
    static List<Class<?>> types;
    static Storable stor1;
    static Storable stor2;

    @Before
    public void setUp() throws IOException {
        Class<?>[] typesAsArray = {Integer.class, Long.class, Byte.class, Float.class, Double.class,
                    Boolean.class, String.class};
        Object[] valuesAsArray = {1, 1L, (byte) 1, (float) 1., (double) 1., true, "asd"};
        types = Arrays.asList(typesAsArray);

        String workingDir = System.getProperty("user.dir") + "/" + "test";
        while (new File(workingDir).exists()) {
            workingDir = workingDir + "1";
        }
        databaseDir = new File(workingDir);
        databaseDir.mkdir();
        MultiFileHashMap provider;
        provider = new MultiFileHashMap(workingDir);
        table = provider.createTable("table", types);

        types = Arrays.asList(typesAsArray);
        stor1 = new Storable(types, Arrays.asList(valuesAsArray));
        valuesAsArray[0] = 2;
        stor2 = new Storable(types, Arrays.asList(valuesAsArray));

    }

    @Test
    public void getNameTest() {
        assertEquals(table.getName(), "table");
    }

    @Test
    public void noChangeCommitTest() throws IOException {
        table.put("key", stor1);
        table.remove("key");
        assertEquals(table.commit(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalKeyShouldFail() {
        table.put("bad key", stor1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullValueShouldFail() {
        table.put("key", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullKeyShouldFail() {
        table.put(null, stor1);
    }

    @Test
    public void rollbackAndCommitTest() throws IOException {
        table.put("key1", stor1);
        table.commit();
        table.put("key1", stor2);
        table.put("key2", stor1);
        table.remove("key3");
        assertEquals(table.rollback(), 2);
        assertEquals(table.size(), 1);
    }

    @Test
    public void sizeTest() {
        table.put("key1", stor1);
        table.put("key2", stor1);
        table.remove("key1");
        assertEquals(table.size(), 1);
    }

    @After
    public void tearDown() throws Exception {
        try {
            Utils.deleteDirectory(databaseDir);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
