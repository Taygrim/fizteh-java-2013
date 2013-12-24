package ru.fizteh.fivt.students.drozdowsky.databaseTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.students.drozdowsky.database.MultiFileHashMap;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MultiFileHashMapTest {
    MultiFileHashMap provider;
    File databaseDir;
    public static List<Class<?>> types;


    @Before
    public void setUp() throws IOException {
        Class<?>[] typesAsArray = {Integer.class, Long.class, Byte.class, Float.class, Double.class,
                    Boolean.class, String.class};
        types = Arrays.asList(typesAsArray);

        String workingDir = System.getProperty("user.dir") + "/" + "test";
        while (new File(workingDir).exists()) {
            workingDir = workingDir + "1";
        }
        databaseDir = new File(workingDir);
        databaseDir.mkdir();
        provider = new MultiFileHashMap(workingDir);
    }

    @Test(expected = IOException.class)
    public void badDirectoryPathShouldFail() throws IOException {
        MultiFileHashMap badProvider = new MultiFileHashMap("abacaba");
    }

    @Test
    public void createAndGetForSameTableShouldBeEqual() throws IOException {
        assertEquals(provider.createTable("table", types), provider.getTable("table"));
    }

    @Test
    public void createGetRemoveTest() throws IOException {
        assertNotNull(provider.createTable("table", types));
        assertNotNull(provider.getTable("table"));
        provider.removeTable("table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createNullShouldFail() throws IOException {
        provider.createTable(null, types);
    }

    @Test
    public void createExistingShouldBeNull() throws IOException {
        provider.createTable("table", types);
        assertNull(provider.createTable("table", types));
    }


    @Test(expected = IllegalArgumentException.class)
    public void getNullShouldFail() {
        provider.getTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNullShouldFail() throws IOException {
        provider.removeTable(null);
    }

    @Test(expected = IllegalStateException.class)
    public void removeNonExistentShouldFail() throws IOException {
        provider.removeTable("nothing");
    }

    @After
    public void tearDown() {
        try {
            Utils.deleteDirectory(databaseDir);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
