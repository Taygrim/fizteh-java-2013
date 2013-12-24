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

public class MultiFileHashMap implements TableProvider, AutoCloseable {
    private File dir;
    private HashMap<String, FileHashMap> database;
    private boolean exists;

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
        exists = true;
    }

    public String toString() {
        return getClass().getSimpleName() + "[" + dir.getAbsolutePath() + "]";
    }

    @Override
    public FileHashMap getTable(String name) {
        checkExistence();
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            if (database.get(name).isClosed()) {
                try {
                    File temp = new File(dir.getAbsoluteFile() + File.separator + name);
                    FileHashMap base = new FileHashMap(temp);
                    database.put(name, base);
                } catch (IOException e) {
                    return null;
                }
            }
            return database.get(name);
        } else {
            return null;
        }
    }

    @Override
    public FileHashMap createTable(String name, List<Class<?>> columnTypes) throws IOException {
        checkExistence();
        if (!Utils.isValidTablename(name) || columnTypes == null || columnTypes.size() == 0) {
            throw new IllegalArgumentException();
        }
        for (Class<?> columnType : columnTypes) {
            if (columnType == null || Utils.classToName(columnType) == null) {
                throw new IllegalArgumentException();
            }
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
        checkExistence();
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (database.containsKey(name)) {
            database.get(name).removeTable();
            Utils.deleteDirectory(new File(dir.getCanonicalPath() + File.separator + name));
            database.remove(name);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        checkExistence();
        return new Storable(Utils.getTableTypes(table), value);
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        checkExistence();
        if (Storable.validStoreable(value, Utils.getTableTypes(table))) {
            return new Storable(Utils.getTableTypes(table), value).toString();
        } else {
            throw new ColumnFormatException();
        }
    }

    @Override
    public Storeable createFor(Table table) {
        checkExistence();
        return new Storable(Utils.getTableTypes(table));
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        checkExistence();
        return new Storable(Utils.getTableTypes(table), values);
    }

    @Override
    public void close() throws IOException {
        checkExistence();
        for (FileHashMap toClose: database.values()) {
            toClose.close();
        }
        exists = false;
    }

    public void stopUsing(String name) {
        checkExistence();
        if (!Utils.isValidTablename(name)) {
            throw new IllegalArgumentException();
        }
        if (!database.containsKey(name)) {
            throw new IllegalStateException();
        }
    }

    private void checkExistence() {
        if (!exists) {
            throw new IllegalStateException();
        }
    }

}
