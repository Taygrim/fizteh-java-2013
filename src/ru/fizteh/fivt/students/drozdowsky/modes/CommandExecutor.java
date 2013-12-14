package ru.fizteh.fivt.students.drozdowsky.modes;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class CommandExecutor<T> {
    T controller;
    public CommandExecutor(T controller) {
        this.controller = controller;
    }

    public boolean executeCommand(String[] args, Method command) {
        try {
            int size = command.getGenericParameterTypes().length;
            if (args.length == size + 1) {
                String[] newArgs = new String[args.length - 1];
                System.arraycopy(args, 1, newArgs, 0, newArgs.length);
                return (boolean) command.invoke(controller, newArgs);
            } else {
                System.err.println("Not valid number of arguments");
            }
        } catch (IllegalAccessException | IllegalStateException e)  {
            System.err.println(e.getMessage());
        } catch (InvocationTargetException | IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
        return false;
    }

    public boolean execute(String[] args, HashMap<String, Method> commands) {
        Method command = commands.get(args[0]);
        if (command != null) {
            return executeCommand(args, command);
        } else {
            System.err.println(args[0] + ": command not found");
        }
        return false;
    }
}
