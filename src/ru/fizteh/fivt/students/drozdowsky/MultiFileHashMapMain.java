package ru.fizteh.fivt.students.drozdowsky;

import ru.fizteh.fivt.students.drozdowsky.commands.MfhmController;
import ru.fizteh.fivt.students.drozdowsky.database.MfhmProviderFactory;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;
import ru.fizteh.fivt.students.drozdowsky.modes.ModeController;
import ru.fizteh.fivt.students.drozdowsky.database.MultiFileHashMap;

import java.awt.geom.IllegalPathStateException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class MultiFileHashMapMain {

    public static void main(String[] args) {
        MfhmProviderFactory factory = new MfhmProviderFactory();
        try {
            factory.create("/root/some-not-existing-dir");
        } catch (IOException e) { }
        String dbDirectory = System.getProperty("fizteh.db.dir");
        if (dbDirectory == null) {
            System.err.println("No database location");
            System.exit(1);
        }
        if (!new File(dbDirectory).isDirectory()) {
            System.err.println(dbDirectory + ": not a directory");
            System.exit(1);
        }
        String[] commandNames = {"create", "drop", "use", "put", "get", "remove", "exit", "size", "commit", "rollback"};
        HashMap<String, Method> map = Utils.getMethods(commandNames, MfhmController.class);
        try {
            MfhmController db = new MfhmController(new MultiFileHashMap(dbDirectory),
                                                   (args.length == 0 ? System.out : System.err));
            ModeController<MfhmController> start = new ModeController<>(db);
            start.execute(map, args);
        } catch (IllegalStateException | IllegalPathStateException | IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
