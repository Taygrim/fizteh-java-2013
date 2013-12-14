package ru.fizteh.fivt.students.drozdowsky.database;

import java.io.File;
import java.io.IOException;
import java.util.*;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

public class FileHashMap implements Table {
    private static final int NDIRS = 16;
    private static final int NFILES = 16;

    private boolean exists;
    private File db;
    private FileMap[][] base;
    private FileMap[][] baseBackUp;
    private List<Class<?>> types;

    public FileHashMap(File db) throws IOException {
        this.db = db;
        exists = true;
        base = new FileMap[NDIRS][NFILES];
        types = new ArrayList<>();
        readDB();
        baseBackUp = new FileMap[NDIRS][NFILES];
        copy(baseBackUp, base);
    }

    public String getName() {
        checkExistence();
        return db.getName();
    }

    public Storable get(String key) {
        checkExistence();
        if (!Utils.isValid(key)) {
            throw new IllegalArgumentException();
        }
        int nDir = getDirNum(key);
        int nFile = getFileNum(key);
        return base[nDir][nFile].get(key);
    }

    public Storable put(String key, Storeable value) throws ColumnFormatException {
        checkExistence();
        if (value == null || !Utils.isValid(key)) {
            throw new IllegalArgumentException();
        }

        int nDir = getDirNum(key);
        int nFile = getFileNum(key);

        Storable myValue = new Storable(types, value);
        return base[nDir][nFile].put(key, myValue);
    }

    public Storable remove(String key) {
        checkExistence();
        if (!Utils.isValid(key)) {
            throw new IllegalArgumentException();
        }
        int nDir = getDirNum(key);
        int nFile = getFileNum(key);
        return base[nDir][nFile].remove(key);
    }

    public int size() {
        checkExistence();
        int result = 0;
        for (int i = 0; i < NDIRS; i++) {
            for (int j = 0; j < NDIRS; j++) {
                result += base[i][j].size();
            }
        }
        return result;
    }

    public int commit() throws IOException {
        checkExistence();
        int result = difference();
        writeDB();
        copy(baseBackUp, base);
        return result;
    }

    public int rollback() {
        checkExistence();
        int result = difference();
        copy(base, baseBackUp);
        return result;
    }

    public int getColumnsCount() {
        checkExistence();
        return types.size();
    }

    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        checkExistence();
        if (columnIndex < 0 || columnIndex >= types.size()) {
            throw new IndexOutOfBoundsException();
        }
        return types.get(columnIndex);
    }

    public int difference() {
        checkExistence();
        int result = 0;
        for (int i = 0; i < NDIRS; i++) {
            for (int j = 0; j < NDIRS; j++) {
                result += compare(base[i][j], baseBackUp[i][j]);
            }
        }
        return result;
    }

    private int compare(FileMap a, FileMap b) {
        int result = 0;
        Set<String> tmp = new TreeSet<>(a.getKeys());
        tmp.removeAll(b.getKeys());
        result += tmp.size();
        tmp = new TreeSet<>(b.getKeys());
        tmp.removeAll(a.getKeys());
        result += tmp.size();

        for (String x : a.getKeys()) {
            if (b.getKeys().contains(x)) {
                if (!a.get(x).equals(b.get(x))) {
                    result++;
                }
            }
        }
        return result;
    }

    public void checkExistence() {
        if (!exists) {
            throw new IllegalStateException();
        }
    }

    public void close() throws IOException {
        writeDB();
    }

    public void removeTable() {
        exists = false;
    }

    private int getDirNum(String key) {
        if (!Utils.isValid(key)) {
            throw new IllegalArgumentException();
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= -1;
        }
        return b % 16;
    }

    private int getFileNum(String key) {
        if (!Utils.isValid(key)) {
            throw new IllegalArgumentException();
        }
        byte b = key.getBytes()[0];
        if (b < 0) {
            b *= -1;
        }
        return (b / 16) % 16;
    }

    private void readTypes() throws IOException {
        File typeDescription = new File(db.toString() + File.separator + "signature.tsv");
        if (typeDescription.exists() && !typeDescription.isFile()) {
            throw new IOException(typeDescription.getAbsolutePath() + ": Not a file");
        }

        if (!typeDescription.exists()) {
            throw new IOException(db.getAbsolutePath() + ": no signature.tsv");
        }

        Scanner scanner = new Scanner(typeDescription);
        while (scanner.hasNext()) {
            String type = scanner.next();
            if (Utils.nameToClass(type) != null) {
                types.add(Utils.nameToClass(type));
            } else {
                throw new IOException(typeDescription.getAbsolutePath() + ": Not valid format");
            }
        }

    }

    private void readDB() throws IOException {
        if (db.exists() && !db.isDirectory()) {
            throw new IOException(db.getAbsolutePath() + ": Not a directory");
        }

        readTypes();

        for (int i = 0; i < NDIRS; i++) {
            for (int j = 0; j < NFILES; j++) {
                base[i][j] = new FileMap(types);
            }
        }

        File[] directories = db.listFiles();
        for (File directory : (directories != null ? directories : new File[0])) {
            if (directory.getName().equals("signature.tsv")) {
                continue;
            }
            int nDir = dirNameInRange(directory.getName(), NDIRS);
            if (nDir == -1 || !(directory.isDirectory())) {
                throw new IllegalStateException(db.getAbsolutePath() + ": Not valid database " + directory.getName());
            }

            File[] files = directory.listFiles();
            for (File file : (files != null ? files : new File[0])) {
                int nFile = fileNameInRange(file.getName(), NFILES);
                if (nFile == -1 || !(file.isFile())) {
                    throw new IllegalStateException(db.getAbsolutePath() + ": Not valid database " + file.getName());
                }

                base[nDir][nFile].read(file);
                Set<String> keys = base[nDir][nFile].getKeys();
                for (String key : keys) {
                    int realNDir = getDirNum(key);
                    int realNFile = getFileNum(key);
                    if (!(nDir == realNDir && nFile == realNFile)) {
                        throw new IllegalStateException(db.getAbsolutePath() + ": Not valid database");
                    }
                }
            }
        }
    }

    private void writeDB() throws IOException {
        for (int i = 0; i < NDIRS; i++) {
            File dirPath = new File(db.getAbsolutePath() + File.separator + Integer.toString(i) + ".dir");
            if (!dirPath.exists() && !dirPath.mkdir()) {
                throw new IOException(dirPath.getAbsolutePath() + ": Permission denied");
            }
            for (int j = 0; j < NFILES; j++) {
                if (base[i][j] != null) {
                    File filePath = new File(dirPath.getAbsolutePath() + File.separator + Integer.toString(j) + ".dat");
                    if (!filePath.exists()) {
                        try {
                            if (!filePath.createNewFile()) {
                                throw new IOException(filePath.getAbsolutePath() + ": Permission denied");
                            }
                        } catch (IOException e) {
                            throw new IOException(filePath.getAbsolutePath() + ": Permission denied");
                        }
                    }
                    if (base[i][j].getKeys().size() == 0) {
                        if (filePath.exists() && !filePath.delete()) {
                            throw new IOException(filePath.getAbsolutePath() + ": Permission denied");
                        }
                    } else {
                        base[i][j].write(filePath);
                    }
                }
            }
            if (dirPath.exists() && dirPath.list().length == 0 && !dirPath.delete()) {
                throw new IOException(dirPath.getAbsolutePath() + ": Permission denied");
            }
        }
    }

    private void copy(FileMap[][] a, FileMap[][] b) {
        for (int i = 0; i < NDIRS; i++) {
            for (int j = 0; j < NFILES; j++) {
                a[i][j] = b[i][j].copy();
            }
        }
    }

    private int dirNameInRange(String s, int range) {
        for (int i = 0; i < range; i++) {
            if ((Integer.toString(i) + ".dir").equals(s)) {
                return i;
            }
        }
        return -1;
    }

    private int fileNameInRange(String s, int range) {
        for (int i = 0; i < range; i++) {
            if ((Integer.toString(i) + ".dat").equals(s)) {
                return i;
            }
        }
        return -1;
    }
}
