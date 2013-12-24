package ru.fizteh.fivt.students.drozdowsky.database;

import ru.fizteh.fivt.storage.structured.TableProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MfhmProviderFactory implements TableProviderFactory, AutoCloseable {

    private List<MultiFileHashMap> created;

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
        created.add(new MultiFileHashMap(dir));
        return created.get(created.size() - 1);
    }

    public void close() throws IOException {
        for (MultiFileHashMap toClose: created) {
            toClose.close();
        }
    }
}
