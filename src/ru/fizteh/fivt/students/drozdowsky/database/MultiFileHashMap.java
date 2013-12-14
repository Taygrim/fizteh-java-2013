package ru.fizteh.fivt.students.drozdowsky.database;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;

public class MultiFileHashMap implements TableProvider {
    private File dir;
    private HashMap<String, FileHashMap> database;

    public MultiFileHashMap(String workingDir) throws IOException {
        database = new HashMap<>();
        this.dir = new File(workingDir);
        if (!(dir.exists())) {
            throw new IOException();
        }
        String[] content = dir.list();
        for (String directory : content) {
            File temp = new File(dir.getAbsoluteFile() + File.separator + directory);
            FileHashMap base = new FileHashMap(temp);
            database.put(directory, base);
        }
    }

    @Override
    public FileHashMap getTable(String name) {
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            return database.get(name);
        } else {
            return null;
        }
    }

    @Override
    public FileHashMap createTable(String name, List<Class<?>> columnTypes) throws IOException {
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            return null;
        } else {
            File newTable = new File(dir.getAbsolutePath() + File.separator + name);
            if (!newTable.mkdir()) {
                throw new IOException(newTable.getAbsolutePath());
            }
            File signature = new File(newTable.getAbsolutePath() + File.separator + "signature.tsv");
            if (!signature.createNewFile()) {
                throw new IOException(signature.getAbsolutePath());
            }

            FileOutputStream signatureOutput = new FileOutputStream(signature);
            for (Class<?> columnType : columnTypes) {
                if (Utils.classToName(columnType) != null) {
                    signatureOutput.write((Utils.classToName(columnType) + " ").getBytes());
                } else {
                    throw new IllegalArgumentException();
                }
            }

            database.put(name, new FileHashMap(newTable));
            return database.get(name);
        }
    }

    @Override
    public void removeTable(String name) throws IOException {
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            database.get(name).removeTable();
            Utils.deleteDirectory(dir);
            database.remove(name);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        return new Storable(Utils.getTableTypes(table), value);
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        if (Storable.validStoreable(value, Utils.getTableTypes(table))) {
            return new Storable(Utils.getTableTypes(table), value).toString();
        } else {
            throw new ColumnFormatException();
        }
    }

    @Override
    public Storeable createFor(Table table) {
        return new Storable(Utils.getTableTypes(table));
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        return new Storable(Utils.getTableTypes(table), values);
    }

    public void stopUsing(String name) {
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            database.put(name, null);
        } else {
            throw new IllegalStateException();
        }
    }
}
