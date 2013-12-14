package ru.fizteh.fivt.students.drozdowsky.utils;

import ru.fizteh.fivt.storage.structured.Table;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Utils {
    public static String[] scanArgs(Scanner in) {
        System.out.print("$ ");
        String temp;
        if (!in.hasNextLine()) {
            System.exit(0);
        }
        temp = in.nextLine().trim();
        if (!temp.isEmpty()) {
            return new String[]{temp};
        } else {
            return new String[0];
        }
    }

    private static boolean in(String what, String[] where) {
        for (String wh : where) {
            if (what.equals(wh)) {
                return true;
            }
        }
        return false;
    }

    public static ArrayList<String[]> parse(String[] args, String[] noWSCommands) {
        args[args.length - 1] = args[args.length - 1] + ";";
        ArrayList<String[]> result = new ArrayList<>();
        ArrayList<String> tempArgs = new ArrayList<>();
        boolean noWS = false;
        for (String arg : args) {
            int last = -1;
            for (int i = 0; i < arg.length(); i++) {
                if (!noWS && arg.charAt(i) == ' ' || arg.charAt(i) == '\t') {
                    if (last + 1 != i) {
                        tempArgs.add(arg.substring(last + 1, i).trim());
                        if (tempArgs.size() == 2 && in(tempArgs.get(0), noWSCommands)) {
                            noWS = true;
                        }
                    }
                    last = i;
                }
                if (arg.charAt(i) == ';') {
                    if (last + 1 != i) {
                        tempArgs.add(arg.substring(last + 1, i).trim());
                    }
                    if (tempArgs.size() != 0) {
                        result.add(tempArgs.toArray(new String[tempArgs.size()]));
                    }
                    last = i;
                    tempArgs.clear();
                    noWS = false;
                }
            }
            if (last + 1 != arg.length()) {
                tempArgs.add(arg.substring(last + 1, arg.length()).trim());
            }
        }
        return result;
    }

    public static HashMap<String, Method> getMethods(String[] commandNames, Class commandClass) {
        HashMap<String, Method> map = new HashMap<>();
        Method[] methods = commandClass.getMethods();
        for (Method method : methods) {
            if (in(method.getName(), commandNames)) {
                map.put(method.getName(), method);
            }
        }
        return map;
    }

    public static boolean isValid(String name) {
        return !(name == null || name.equals("") || name.contains(System.lineSeparator())
                || name.contains(" ") || name.contains("\t"));
    }

    public static boolean isValidTablename(String name) {
        return isValid(name) && !name.contains(File.separator) && !name.contains("\\");
    }

    public static void deleteDirectory(File toDelete) throws IOException {
        File[] files = toDelete.listFiles();
        if (files != null) {
            for (File f: files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    if (!f.delete()) {
                        throw new IOException();
                    }
                }
            }
        }
        if (!toDelete.delete()) {
            throw new IOException();
        }
    }

    public static ArrayList<Class<?>> getTableTypes(Table table) {
        ArrayList<Class<?>> result = new ArrayList<>();
        for (int i = 0; i < table.getColumnsCount(); i++) {
            result.add(table.getColumnType(i));
        }
        return result;
    }
}
