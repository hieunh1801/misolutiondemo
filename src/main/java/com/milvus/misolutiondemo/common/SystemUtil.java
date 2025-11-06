package com.milvus.misolutiondemo.common;

public class SystemUtil {
    public enum OS {
        WINDOWS, LINUX, MACOS, SOLARIS
    }

    private static OS os = null;

    public static OS getOS() {
        if (os == null) {
            String operSys = System.getProperty("os.name").toLowerCase();
            if (operSys.contains("win")) {
                os = OS.WINDOWS;
            } else if (operSys.contains("nix") || operSys.contains("nux")
                    || operSys.contains("aix")) {
                os = OS.LINUX;
            } else if (operSys.contains("mac")) {
                os = OS.MACOS;
            } else if (operSys.contains("sunos")) {
                os = OS.SOLARIS;
            }
        }
        return os;
    }

    public static boolean isWindows() {
        return OS.WINDOWS == getOS();
    }

    public static boolean isMac() {
        return OS.MACOS == getOS();
    }

    public static boolean isLinux() {
        return OS.LINUX == getOS();
    }
}
