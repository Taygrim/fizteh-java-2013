package ru.fizteh.fivt.students.valentinbarishev.filemap;

import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.storage.structured.TableProviderFactory;

import java.io.File;
import java.io.IOException;

public class MyTableProviderFactory implements TableProviderFactory {

    @Override
    public TableProvider create(String dir) {
        if (dir == null) {
            throw new IllegalArgumentException("Dir cannot be null");
        }

        File tableDirFile = new File(dir);

        if (!tableDirFile.exists()) {
            if (!tableDirFile.mkdirs()) {
                try {
                    throw new IOException("Cannot create directory! " + tableDirFile.getCanonicalPath());
                } catch (IOException e) {
                    throw new RuntimeException("Mkdirs failed", e);
                }
            }
        }

        if (!tableDirFile.isDirectory()) {
            throw new IllegalArgumentException("Wrong dir " + dir);
        }

        return new DataBaseTable(dir);
    }
}
