package ru.fizteh.fivt.students.drozdowsky.databaseTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.students.drozdowsky.database.MfhmProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;

public class MfhmProviderFactoryTest {
    public MfhmProviderFactory factory;
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
    public void validCreateTest() throws IOException {
        TableProvider provider = factory.create(databaseDir.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullCreateTestShouldFail()  throws IOException {
        TableProvider provider = factory.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyCreateTestShouldFail() throws IOException {
        TableProvider provider = factory.create("");
    }

    @Test(expected = IOException.class)
    public void invalidNameShouldFail() throws IOException {
        TableProvider badProvider = factory.create("aba/aba");
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
