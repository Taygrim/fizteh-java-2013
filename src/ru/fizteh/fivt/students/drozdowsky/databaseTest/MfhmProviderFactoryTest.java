package ru.fizteh.fivt.students.drozdowsky.databaseTest;

import org.junit.*;
import org.junit.Test;
import ru.fizteh.fivt.students.drozdowsky.database.MfhmProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.database.MultiFileHashMap;

import java.io.File;
import java.io.IOException;

public class MfhmProviderFactoryTest {
    static MfhmProviderFactory factory;
    static File databaseDir;

    @Before
    public void setUp() {
        factory = new MfhmProviderFactory();
        String workingDir = System.getProperty("user.dir") + "/" + "test";
        while (new File(workingDir).exists()) {
            workingDir = workingDir + "1";
        }
        databaseDir = new File(workingDir);
        databaseDir.mkdir();
    }

    @Test
    public void legalCreateTest() {
        MultiFileHashMap provider = factory.create(databaseDir.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createNullTestShouldFail() {
        MultiFileHashMap provider = factory.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidNameShouldFail() {
        MultiFileHashMap badProvider = factory.create("aba/aba");
    }

    @After
    public void tearDown() {
        try {
            ShellController.deleteDirectory(databaseDir);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
