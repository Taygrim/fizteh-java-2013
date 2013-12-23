package ru.fizteh.fivt.students.drozdowsky.commands;

import ru.fizteh.fivt.students.drozdowsky.database.FileHashMap;
import ru.fizteh.fivt.students.drozdowsky.database.MultiFileHashMap;
import ru.fizteh.fivt.students.drozdowsky.utils.Storable;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.awt.geom.IllegalPathStateException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class MfhmController {

    FileHashMap currentdb;
    MultiFileHashMap multiFileHashMap;
    PrintStream errorsOuptut;

    public MfhmController(MultiFileHashMap multiFileHashMap) {
        this.multiFileHashMap = multiFileHashMap;
        errorsOuptut = System.err;
        currentdb = null;
    }

    public MfhmController(MultiFileHashMap multiFileHashMap, PrintStream errorsOutput) {
        this.multiFileHashMap = multiFileHashMap;
        this.errorsOuptut = errorsOutput;
        currentdb = null;
    }

    public boolean create(String tablename, String types) {
        try {
            if (types.length() == 0 || !(types.charAt(0) == '(' && types.charAt(types.length() - 1) == ')')) {
                errorsOuptut.println("create: not valid arguments");
                return false;
            }

            types = types.substring(1, types.length() - 1);
            String[] splittedTypes = types.split("[ \t]");

            if (splittedTypes.length == 0) {
                errorsOuptut.println("wrong type (create)");
                return false;
            }

            List<Class<?>> typesAsClasses = new ArrayList<>();
            for (String splittedType : splittedTypes) {
                if (Utils.nameToClass(splittedType) == null) {
                        errorsOuptut.println("wrong type (create)");
                        return false;
                } else {
                    typesAsClasses.add(Utils.nameToClass(splittedType));
                }
            }

            if (multiFileHashMap.createTable(tablename, typesAsClasses) != null) {
                System.out.println("created");
            } else {
                System.out.println(tablename + " exists");
            }
            return true;
        } catch (IllegalStateException | IllegalPathStateException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        } catch (IOException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        }
    }

    public boolean drop(String name) {
        try {
            if (currentdb != null && name.equals(currentdb.getName())) {
                currentdb = null;
            }
            multiFileHashMap.removeTable(name);
            System.out.println("dropped");
            return true;
        } catch (IllegalPathStateException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        } catch (IllegalStateException e) {
            System.out.println(name + " not exists");
            return true;
        } catch (IOException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        }
    }

    public boolean use(String name) {
        try {
            if (currentdb != null && currentdb.difference() > 0) {
                errorsOuptut.println(currentdb.difference() + " unsaved changes");
                return false;
            }
            if (multiFileHashMap.getTable(name) != null) {
                if (currentdb != null) {
                    multiFileHashMap.stopUsing(currentdb.getName());
                }
                currentdb = multiFileHashMap.getTable(name);
                System.out.println("using " + name);
            } else {
                System.out.println(name + " not exists");
            }
            return true;
        } catch (IllegalStateException | IllegalPathStateException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        }
    }

    public boolean size() {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        System.out.println(currentdb.size());
        return true;
    }

    public boolean put(String key, String value) {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        try {
            Storable result = currentdb.put(key, new Storable(Utils.getTableTypes(currentdb), value));
            if (result != null) {
                System.out.println("overwrite\n" + result.toString());
            } else {
                System.out.println("new");
            }
            return true;

        } catch (ParseException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        }
    }

    public boolean get(String key) {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        Storable result = currentdb.get(key);
        if (result != null) {
            System.out.println("found\n" + result.toString());
        } else {
            System.out.println("not found");
        }
        return true;
    }

    public boolean remove(String key) {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        Storable result = currentdb.remove(key);
        if (result != null) {
            System.out.println("removed");
        } else {
            System.out.println("not found");
        }
        return true;
    }

    public boolean exit() {
        if (currentdb != null) {
            try {
                currentdb.close();
            } catch (IOException e) {
                errorsOuptut.println(e.getMessage());
            }
        }
        System.exit(0);
        return true;
    }

    public boolean commit() {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        try {
            System.out.println(currentdb.commit());
        } catch (IOException e) {
            errorsOuptut.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean rollback() {
        if (currentdb == null) {
            System.out.println("no table");
            return false;
        }
        System.out.println(currentdb.rollback());
        return true;
    }
}
