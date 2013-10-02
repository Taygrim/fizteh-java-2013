package ru.fizteh.fivt.students.kislenko.shell;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class CommandCd implements Command {
    public void run(String s) throws IOException {
        String[] args = s.split("  *");
        if (args.length > 2) {
            throw new IOException("cd: Too many arguments.");
        } else if (args.length < 2) {
            throw new IOException("cd: Too few arguments.");
        }
        String path = args[1];
        Path absolutePath = Shell.absolutePath;
        absolutePath = absolutePath.resolve(path);
        File newDir = new File(absolutePath.toString());
        if (!newDir.isDirectory()) {
            throw new FileNotFoundException("cd: Directory is not exist.");
        }
        Shell.absolutePath = absolutePath.normalize();
    }
}