package ru.fizteh.fivt.students.drozdowsky.pathController;

import java.io.*;

public class ShellCommands {
    public static boolean cd(PathController path, String[] args) {
        try {
            if (args.length != 2) {
                System.err.println("usage: cd <absolute path|relative path>");
                return false;
            }
            PathController test = new PathController(path);

            test.changePath(args[1]);

            if (test.isDirectory()) {
                path.changePath(args[1]);
            } else {
                if (!test.exists()) {
                    System.err.println("cd: " + args[1] + ": No such file or directory");
                    return false;
                }
                System.err.println("cd: " + args[1] + ": Not a directory");
                return false;
            }
            return true;
        } catch (IOException e) {
            System.err.println("cd: " + args[1] + ": " + e.getMessage());
        }
        return false;
    }

    public static boolean cp(PathController path, String[] args) {
        if (args.length != 3) {
            System.err.println("usage: cp file|directory file|directory");
            return false;
        }
        try {
            PathController pathFrom = new PathController(path);
            PathController pathTo = new PathController(path);
            pathFrom.changePath(args[1]);
            pathTo.changePath(args[2]);

            if (pathFrom.getPath().equals(pathTo.getPath())) {
                System.err.println(args[1] + " and " + args[2] + " are identical (not copied).");
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

    public static boolean dir(PathController path, String[] args) {
        try {
            if (args.length != 1) {
                System.err.println("usage: dir");
                return false;
            }

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

    public static boolean mkdir(PathController path, String[] args) {
        if (args.length != 2) {
            System.err.println("usage: mkdir directory");
            return false;
        }
        try {
            PathController tempPath = new PathController(path);
            tempPath.changePath(args[1]);
            File newDirectory = tempPath.getPath();

            if (!newDirectory.mkdir() && newDirectory.exists()) {
                System.err.println("mkdir: " + args[1] + ": " + "File exists");
                return false;
            }
            return true;
        } catch (SecurityException e) {
            System.err.println("mkdir: " + args[1] + ": " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mkdir: " + args[1] + ": " + e.getMessage());
        }
        return false;
    }

    public static boolean mv(PathController path, String[] args) {
        if (args.length != 3) {
            System.err.println("usage: mv file|directory file|directory");
            return false;
        }
        try {
            PathController pathFrom = new PathController(path);
            PathController pathTo = new PathController(path);
            pathFrom.changePath(args[1]);
            pathTo.changePath(args[2]);

            if (pathFrom.getPath().equals(pathTo.getPath())) {
                System.err.println(args[1] + " and " + args[2] + " are identical (not moved).");
                return false;
            }

            if (cp(path, args)) {
                String[] args2 = new String[2];
                args2[1] = args[1];
                args2[0] = args[0];
                return rm(path, args2);
            }
            return false;
        } catch (SecurityException e) {
            System.err.println("mv: " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mv: " +  e.getMessage());
        }
        return false;
    }

    public static boolean pwd(PathController path, String[] args) {
        System.out.println(path.toString());
        return true;
    }

    public static boolean rm(PathController path, String[] args) {
        if (args.length != 2) {
            System.err.println("usage: rm file|directory");
            return false;
        }
        try {
            PathController tempPath = new PathController(path);
            tempPath.changePath(args[1]);
            File toDelete = tempPath.getPath();

            if (!toDelete.exists()) {
                System.err.println("rm: " + args[1] + ": " + "No such file or directory");
                return false;
            }
            if (toDelete.isDirectory()) {
                deleteDirectory(toDelete);
            }

            toDelete.delete();
            return true;
        } catch (SecurityException e) {
            System.err.println("mkdir: " + args[1] + ": " + "Permission denied");
        } catch (IOException e) {
            System.err.println("mkdir: " + args[1] + ": " + e.getMessage());
        }
        return false;
    }

    public static boolean exit(PathController path, String[] args) {
        if (args.length != 1) {
            System.err.println("usage: exit");
            return false;
        }
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

    private static void deleteDirectory(File toDelete) throws IOException {
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