package ru.fizteh.fivt.students.drozdowsky;

import ru.fizteh.fivt.students.drozdowsky.commands.ShellController;
import ru.fizteh.fivt.students.drozdowsky.modes.ModeController;
import ru.fizteh.fivt.students.drozdowsky.utils.Utils;

import java.lang.reflect.Method;
import java.util.HashMap;

public class ShellMain {
    public static void main(String[] args) {
        String[] commandNames = {"cd", "cp", "dir", "mkdir", "mv", "pwd", "rm", "exit"};
        HashMap<String, Method> map = Utils.getMethods(commandNames, ShellController.class);
        ShellController path = new ShellController();
        ModeController<ShellController> start = new ModeController<>(path);
        start.execute(map, args);
    }
}
