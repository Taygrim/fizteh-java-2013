package ru.fizteh.fivt.students.kamilTalipov.shell;

public class ShellRunner {
    public static void main(String[] args) {
        Command[] commands = {new ChangeDir(),
                            new MakeDir(),
                            new PrintWorkingDir(),
                            new Remove(),
                            new Copy(),
                            new Move(),
                            new PrintDirContain(),
                            new Exit()};
        Shell shell = new Shell(commands);
        try {
            if (args.length == 0) {
                shell.interactiveMode();
            } else {
                shell.packageMode(args);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
