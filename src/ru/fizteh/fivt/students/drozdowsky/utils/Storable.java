package ru.fizteh.fivt.students.drozdowsky.utils;

import org.json.JSONException;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.List;
import org.json.JSONArray;

public class Storable implements Storeable{

    private String[] values;
    private Class<?>[] types;

    public Storable(List<Class<?>> types) {
        if (types == null) {
            throw new IllegalArgumentException("Got null signature");
        }
        this.values = new String[types.size()];
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
        if (types.size() != this.values.length) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = 0; i < types.size(); i++) {
            if (!values.get(i).getClass().equals(this.types[i])) {
                throw new ColumnFormatException("wrong type (" + "in position" + i + ")");
            } else {
                this.values[i] = values.get(i).toString();
            }
        }
    }

    public Storable(List<Class<?>> types, String values) throws ParseException {
        this(types);
        try {
            JSONArray tempValues = new JSONArray(values);
            if (types.size() != this.values.length) {
                throw new ParseException("wrong type (" + "Not valid size" + ")", values.length());
            }

            for (int i = 0; i < types.size(); i++) {
                this.values[i] = tempValues.get(i).toString();
                getObjectWithTypeAt(i, types.get(i));
            }
        } catch (JSONException e) {
            throw new ParseException("wrong type (" + e.getCause().getMessage() + ")",
                    e.getCause().toString().indexOf('1'));
        }

    }

    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= values.length) {
            throw new IndexOutOfBoundsException();
        }
        if (value == null) {
            values[columnIndex] = null;
        } else {
            if (!types[columnIndex].equals(value.getClass())) {
                throw new ColumnFormatException();
            } else {
                values[columnIndex] = value.toString();
            }
        }
    }

    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= values.length) {
            throw new IndexOutOfBoundsException();
        }
        return getObjectWithTypeAt(columnIndex, types[columnIndex]);
    }

    @Override
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Integer) getObjectWithTypeAt(columnIndex, Integer.class);
    }

    @Override
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Long) getObjectWithTypeAt(columnIndex, Long.class);
    }

    @Override
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Byte) getObjectWithTypeAt(columnIndex, Byte.class);
    }

    @Override
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Float) getObjectWithTypeAt(columnIndex, Float.class);
    }

    @Override
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Double) getObjectWithTypeAt(columnIndex, Double.class);
    }

    @Override
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (Boolean) getObjectWithTypeAt(columnIndex, Boolean.class);
    }

    @Override
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        return (String) getObjectWithTypeAt(columnIndex, String.class);
    }

    private Object getObjectWithTypeAt(int columnIndex, Class<?> type) {
        if (columnIndex < 0 || columnIndex >= values.length) {
            throw new IndexOutOfBoundsException();
        }

        if (values[columnIndex] == null || values[columnIndex].equals("null")) {
            return null;
        }

        try {
            return type.getConstructor(String.class).newInstance(values[columnIndex]);
        } catch (NumberFormatException e) {
            throw new ColumnFormatException(e);
        } catch (InvocationTargetException | NoSuchMethodException
                | InstantiationException | IllegalAccessException e) {
            throw  new ColumnFormatException(e);
        }
    }

    public String toString() {
        JSONArray result = new JSONArray();
        for (int i = 0; i < types.length; i++) {
            result.put(getObjectWithTypeAt(i, types[i]));
        }
        return result.toString();
    }

    public boolean theSame(Storable toCompare) {

        Class<?>[] typesToCompare = toCompare.getTypes();

        if(typesToCompare.length != types.length) {
            return false;
        }

        for (int i = 0; i < types.length; i++) {
            if (!types[i].equals(typesToCompare[i])) {
                return false;
            }
        }

        for (int i = 0; i < values.length; i++) {
            try {
                Object temp1 = toCompare.getColumnAt(i);
                Object temp2 = this.getColumnAt(i);
                if(temp1 == null || temp2 == null) {
                    if(temp1 == null && temp2 == null) {
                        continue;
                    } else {
                        return false;
                    }
                }
                if(!temp1.equals(temp2)) {
                    return false;
                }
            } catch (IndexOutOfBoundsException e) {
                return false;
            }
        }
        return true;
    }

    private Class<?>[] getTypes() {
        return types;
    }

    public static boolean validStoreable(Storeable stor, List<Class<?>> types) {
        for (int i = 0; i < types.size(); i++) {
            try {
                Object temp = stor.getColumnAt(i);
                if (temp != null && !temp.getClass().equals(types.get(i))) {
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
