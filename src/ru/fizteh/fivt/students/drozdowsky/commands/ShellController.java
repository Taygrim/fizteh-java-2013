package ru.fizteh.fivt.students.drozdowsky.commands;

import ru.fizteh.fivt.students.drozdowsky.PathController;

import java.io.*;

public class ShellController {

    PathController path;

    public ShellController() {
        path = new PathController();
    }

    public ShellController(PathController path) {
        this.path = path;
    }

    public boolean cd(String to) {
        try {
            PathController test = new PathController(path);

            test.changePath(to);

            if (test.isDirectory()) {
                path.changePath(to);
            } else {
                if (!test.exists()) {
                    System.err.println("cd: " + to + ": No such file or directory");
                    return false;
                }
                System.err.println("cd: " + to + ": Not a directory");
                return false;
            }
            return true;
        } catch (IOException e) {
            System.err.println("cd: " + to + ": " + e.getMessage());
        }
        return false;
    }

    public boolean cp(String from, String to) {
        try {
            PathController pathFrom = new PathController(path);
            PathController pathTo = new PathController(path);
            pathFrom.changePath(from);
            pathTo.changePath(to);

            if (pathFrom.getPath().equals(pathTo.getPath())) {
                System.err.println(from + " and " + to + " are identical (not copied).");
                return false;
            }

            return copy(pathFrom.getPath(), pathTo.getPath());
        } catch (SecurityException e) {
            System.err.println("cp: " + "Permission denied");
        } catch (IOException e) {
            System.err.println("cp: " + e.getMessage());
        }
        return false;
    }

    public boolean dir() {
        try {
            File totalPath = path.getPath();
            String[] result = totalPath.list();
            if (result == null) {
                return true;
            }

            for (String aResult : result) {
                System.out.println(aResult);
            }
        } catch (SecurityException e) {
            System.out.println(e);
        }
        return true;
    }

    public boolean mkdir(String where) {

        try {
            PathController tempPath = new PathController(path);
            tempPath.changePath(where);
            File newDirectory = tempPath.getPath();

            if (!newDirectory.mkdir() && newDirectory.exists()) {
                System.err.println("mkdir: " + where + ": " + "File exists");
                return false;
            }
            return true;
        } catch (SecurityException e) {
            System.err.println("mkdir: " + where + ": " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mkdir: " + where + ": " + e.getMessage());
        }
        return false;
    }

    public boolean mv(String from, String to) {
        try {
            PathController pathFrom = new PathController(path);
            PathController pathTo = new PathController(path);
            pathFrom.changePath(from);
            pathTo.changePath(to);

            if (pathFrom.getPath().equals(pathTo.getPath())) {
                System.err.println(from + " and " + to + " are identical (not moved).");
                return false;
            }

            if (cp(from, to)) {
                return rm(from);
            }
            return false;
        } catch (SecurityException e) {
            System.err.println("mv: " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mv: " +  e.getMessage());
        }
        return false;
    }

    public boolean pwd() {
        System.out.println(path.toString());
        return true;
    }

    public boolean rm(String what) {
        try {
            PathController tempPath = new PathController(path);
            tempPath.changePath(what);
            File toDelete = tempPath.getPath();

            if (!toDelete.exists()) {
                System.err.println("rm: " + what + ": " + "No such file or directory");
                return false;
            }
            if (toDelete.isDirectory()) {
                deleteDirectory(toDelete);
            }

            toDelete.delete();
            return true;
        } catch (SecurityException e) {
            System.err.println("mkdir: " + what + ": " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mkdir: " + what + ": " + e.getMessage());
        }
        return false;
    }

    public boolean exit() {
        System.exit(0);
        return true;
    }

    private static boolean copy(File from, File to) throws IOException, SecurityException {
        if (!from.exists()) {
            System.err.println("cp: " + from.getName() + ": " + "No such file or directory");
            return false;
        }
        if (from.isDirectory()) {
            if (to.exists() && !to.isDirectory()) {
                System.err.println("cp: " + to.getName() + ": " + "Not a directory");
                return false;
            }
            copyDirectory(from, to);
        } else {
            if (to.exists() && !to.isFile()) {
                System.err.println("cp: " + to.getName() + ": " + "Not a file");
                return false;
            }
            copyFile(from, to);
        }
        return true;
    }

    private static void copyFile(File from, File to) throws IOException, SecurityException {
        InputStream in = new FileInputStream(from);
        OutputStream out = new FileOutputStream(to);
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
    }

    private static void copyDirectory(File from, File to) throws IOException, SecurityException {
        if (!to.exists()) {
            to.mkdir();
        }

        String[] children = from.list();
        for (String aChildren : children) {
            copy(new File(from, aChildren), new File(to, aChildren));
        }
    }

    public static void deleteDirectory(File toDelete) throws IOException {
        File[] files = toDelete.listFiles();
        if (files != null) {
            for (File f: files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    try {
                        f.delete();
                    } catch (SecurityException e) {
                        throw new IOException(f.getAbsolutePath() + " permission denied");
                    }
                }
            }
        }
        toDelete.delete();
    }
}
