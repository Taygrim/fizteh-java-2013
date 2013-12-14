package ru.fizteh.fivt.students.drozdowsky.utils;

import org.json.JSONException;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;

import java.text.ParseException;
import java.util.List;
import org.json.JSONArray;

public class Storable implements Storeable{

    private JSONArray values;
    private Class<?>[] types;

    public Storable(List<Class<?>> types) {
        if (types == null) {
            throw new IllegalArgumentException("Got null signature");
        }
        this.values = new JSONArray(new Object[types.size()]);
        this.types = types.toArray(new Class<?>[types.size()]);
    }

    public Storable(List<Class<?>> types, Storeable stor) throws ColumnFormatException {
        this(types);
        if (!validStoreable(stor, types)) {
            throw new ColumnFormatException();
        }

        for (int i = 0; i < types.size(); i++) {
            this.setColumnAt(i, stor.getColumnAt(i));
        }
    }

    public Storable(List<Class<?>> types, List<?> values) {
        this(types);
        if (types.size() != values.size()) {
            throw new IndexOutOfBoundsException();
        }
        this.values = new JSONArray(values.toArray());
        for (int i = 0; i < types.size(); i++) {
            if (this.values.get(i) != null && this.values.get(i).getClass() != this.types[i]) {
                throw new ColumnFormatException();
            }
        }
    }

    public Storable(List<Class<?>> types, String values) throws ParseException {
        this(types);
        try {
            this.values = new JSONArray(values);
        } catch (JSONException e) {
            throw new ParseException("wrong type (" + e.getCause().getMessage() + ")", e.getCause().toString().indexOf('1'));
        }

        if (types.size() != this.values.length()) {
            throw new ParseException("wrong type (" + "Not valid :" + ")", values.length());
        }

        for (int i = 0; i < types.size(); i++) {
            if (this.values.get(i) != null && this.values.get(i).getClass() != this.types[i]) {
                throw new ColumnFormatException("wrong type (" + "found " + this.values.get(i).getClass().toString()
                        + ", expected" + this.types[i].toString() + ")");
            }
        }
    }

    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= values.length()) {
            throw new IndexOutOfBoundsException();
        }
        if (value == null) {
            values.put(columnIndex, (Object) null);
        } else {
            if (!types[columnIndex].equals(value.getClass())) {
                throw new ColumnFormatException();
            } else {
                values.put(columnIndex, value);
            }
        }
    }

    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= values.length()) {
            throw new IndexOutOfBoundsException();
        }
        return values.get(columnIndex);
    }

    @Override
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Integer) getColumnAt(columnIndex);
    }

    @Override
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Long) getColumnAt(columnIndex);
    }

    @Override
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Byte) getColumnAt(columnIndex);
    }

    @Override
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Float) getColumnAt(columnIndex);
    }

    @Override
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Double) getColumnAt(columnIndex);
    }

    @Override
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (Boolean) getColumnAt(columnIndex);
    }

    @Override
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        checkTypeAt(columnIndex, Integer.class);
        return (String) getColumnAt(columnIndex);
    }

    public int getLength() {
        return values.length();
    }

    private void checkTypeAt(int columnIndex, Class<?> type) {
        if (!types[columnIndex].equals(type)) {
            throw new ColumnFormatException("wrong type (expected " + type.toString() + ", but got "
                    + types[columnIndex].toString() + ")");
        }
    }

    public String toString() {
        return values.toString();
    }

    public static boolean validStoreable(Storeable stor, List<Class<?>> types) {
        for (int i = 0; i < types.size(); i++) {
            try {
                if (!(stor.getColumnAt(i).getClass() == null || stor.getColumnAt(i).getClass().equals(types.get(i)))) {
                    return false;
                }
            } catch (IndexOutOfBoundsException e) {
                return false;
            }
        }
        try {
            stor.getColumnAt(types.size());
        } catch (IndexOutOfBoundsException e) {
            return true;
        }
        return false;
    }

}
