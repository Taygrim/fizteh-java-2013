package ru.fizteh.fivt.students.drozdowsky.database;

import ru.fizteh.fivt.students.drozdowsky.utils.Pair;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FileMap {
    private HashMap<String, Storable> db;
    private List<Class<?>> types;

    static final int BUFFSIZE = 100000;

    public FileMap(List<Class<?>> types) {
        this.types = types;
        db = new HashMap<>();
    }

    private FileMap(HashMap<String, Storable> db, List<Class<?>> types) {
        this.types = types;
        this.db = (HashMap<String, Storable>) db.clone();
    }

    public Storable get(String key) {
        return db.get(key);
    }

    public Storable put(String key, Storable value) {
        Storable result = db.get(key);
        db.put(key, value);
        return result;
    }

    public Storable remove(String key) {
        Storable result = db.get(key);
        db.remove(key);
        return result;
    }

    public int size() {
        return db.size();
    }

    public Set<String> getKeys() {
        return db.keySet();
    }

    protected void read(File dbPath) throws IOException {
        try (FileInputStream inputDB = new FileInputStream(dbPath)) {
            ArrayList<Pair<String, Integer>> offset = new ArrayList<>();

            byte next;
            int byteRead = 0;
            ByteBuffer key = ByteBuffer.allocate(BUFFSIZE);

            while ((next = (byte) inputDB.read()) != -1) {
                if (next == 0) {
                    byte[] sizeBuf = new byte[4];
                    for (int i = 0; i < 4; i++) {
                        if ((sizeBuf[i] = (byte) inputDB.read()) == -1) {
                            throw new IOException(dbPath.getPath() + ": unexpected end of the database");
                        }
                    }
                    int valueSize = ByteBuffer.wrap(sizeBuf).getInt();

                    byte[] keyArray = new byte[key.position()];
                    byteRead += key.position() + 4 + 1;
                    key.clear();
                    key.get(keyArray);
                    offset.add(new Pair<>(new String(keyArray, "UTF-8"), valueSize));
                    key.clear();

                } else {
                    key.put(next);
                }
            }

            if (offset.size() == 0 && key.position() != 0) {
                throw new IOException("No keys found");
            }

            if (offset.size() == 0) {
                return;
            }

            if (offset.get(0).snd != byteRead) {
                throw new IOException("No valid database format");
            }

            for (int i = 0; i < offset.size() - 1; i++) {
                offset.set(i, new Pair<>(offset.get(i).fst, offset.get(i + 1).snd - byteRead));
            }
            int n = offset.size();
            offset.set(n - 1, new Pair<>(offset.get(n - 1).fst, key.position()));

            int prevOffset = 0;
            for (Pair<String, Integer> now: offset) {
                Integer currentOffset = now.snd;
                if (currentOffset <= prevOffset) {
                    throw new IOException("Not valid format");
                } else {
                    ByteBuffer valueBuf = ByteBuffer.allocate(currentOffset - prevOffset);
                    for (int i = prevOffset; i < currentOffset; i++) {
                        valueBuf.put(key.get(i));
                    }
                    prevOffset = currentOffset;
                    String value = new String(valueBuf.array(), "UTF-8");
                    try {
                        db.put(now.fst, new Storable(types, value));
                    } catch (ParseException e) {
                        throw new IOException(e.toString());
                    }
                }
            }
        }
    }

    protected void write(File dbPath) throws IOException {
        try (FileOutputStream out = new FileOutputStream(dbPath)) {
            ArrayList<Integer> length = new ArrayList<>();
            ArrayList<String> values = new ArrayList<>();
            ArrayList<String> keys = new ArrayList<>();
            int totalLength = 0;
            for (String key:db.keySet()) {
                keys.add(key);
                values.add(db.get(key).toString());
                totalLength += key.getBytes("UTF-8").length;
                length.add(db.get(key).toString().getBytes("UTF-8").length);
                totalLength += 4;
                totalLength += "\0".getBytes("UTF-8").length;
            }
            for (int i = 0; i < keys.size(); i++) {
                out.write(keys.get(i).getBytes("UTF-8"));
                out.write("\0".getBytes("UTF-8"));
                out.write(ByteBuffer.allocate(4).putInt(totalLength).array());
                totalLength += length.get(i);
            }
            for (int i = 0; i < keys.size(); i++) {
                out.write(values.get(i).getBytes("UTF-8"));
            }
        }
    }

    protected FileMap copy() {
        return new FileMap(db, types);
    }
}
