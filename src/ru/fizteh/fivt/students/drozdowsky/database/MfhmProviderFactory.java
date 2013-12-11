package ru.fizteh.fivt.students.drozdowsky.database;

import ru.fizteh.fivt.storage.structured.TableProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;

public class MfhmProviderFactory implements TableProviderFactory {
    public MultiFileHashMap create(String dir) {
        if (!Utils.isValid(dir) || !(new File(dir).isDirectory())) {
            throw new IllegalArgumentException();
        }
        try {
            return new MultiFileHashMap(dir);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }
}
