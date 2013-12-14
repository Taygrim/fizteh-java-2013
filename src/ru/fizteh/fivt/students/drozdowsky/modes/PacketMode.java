package ru.fizteh.fivt.students.drozdowsky.modes;

import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

public class PacketMode<T> {
    T controller;
    private static final String[] NOWSCOMMANDS = {"put", "create"};

    public PacketMode(T controller) {
        this.controller = controller;
    }

    public void execute(String[] args, HashMap<String, Method> commands) {
        execute(args, commands, false);
    }

    public void execute(String[] args, HashMap<String, Method> commands, boolean exitOnFailure) {
        ArrayList<String[]> inCommands = Utils.parse(args, NOWSCOMMANDS);

        for (String[] inCommand : inCommands) {
            CommandExecutor<T> cm = new CommandExecutor<T>(controller);
            if (!cm.execute(inCommand, commands)) {
                if (exitOnFailure) {
                    try {
                        Method close = controller.getClass().getMethod("close");
                        if (close != null) {
                            cm.executeCommand(null, close);
                        }
                    } catch (NoSuchMethodException e) {
                        System.err.println(e.getMessage());
                    }
                    System.exit(1);
                } else {
                    return;
                }
            }
        }
    }
}
