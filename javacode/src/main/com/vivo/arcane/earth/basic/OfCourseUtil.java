package com.vivo.arcane.earth.basic;

import java.io.*;

public class OfCourseUtil {
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean isNullOrWhiteSpace(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static boolean equals(String a, String b) {
        return equals(a, b, false);
    }

    public static boolean equals(String a, String b, Boolean ignoreCase) {
        boolean doubleNull = (a == null) && (b == null);
        boolean singleNull = (a == null && b != null) || (a != null && b == null);

        if (doubleNull || singleNull) {
            return doubleNull ? true : false;
        }

        if (ignoreCase) {
            a = a.toLowerCase();
            b = b.toLowerCase();
        }

        return a.equals(b);
    }

    public static boolean mkDirs(String path) {
        return new File(path).mkdirs();
    }

    public static void removeEverything(String path) {
        File fileOrDirectory = new File(path);

        if (!fileOrDirectory.exists()) {
            return;
        }

        if (fileOrDirectory.isDirectory()) {
            File[] elements = fileOrDirectory.listFiles();

            if (elements != null && elements.length > 0) {
                for (File fileOrDirectoryItem : elements) {
                    removeEverything(fileOrDirectoryItem.getAbsolutePath());
                }
            }
        }
        // now we have a file or an empty folder, let's kill it
        fileOrDirectory.delete();
    }
}
