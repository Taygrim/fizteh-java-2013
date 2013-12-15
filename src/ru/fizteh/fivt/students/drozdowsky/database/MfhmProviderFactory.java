package ru.fizteh.fivt.students.drozdowsky.database;

import ru.fizteh.fivt.storage.structured.TableProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;

public class MfhmProviderFactory implements TableProviderFactory {
    public MultiFileHashMap create(String dir) throws IOException {
        if (!Utils.isValid(dir)) {
            throw new IllegalArgumentException(dir);
        }
        if (!(new File(dir).exists())) {
            throw new IOException(dir);
        }
        if (!(new File(dir).isDirectory())) {
            throw new IllegalArgumentException(dir);
        }
        return new MultiFileHashMap(dir);
    }
}
