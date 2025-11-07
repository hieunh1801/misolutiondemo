package com.milvus.misolutiondemo.tableau;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class FileHelper {
    public static boolean createFolder(String folderPath) {
        Path path = Paths.get(folderPath);
        if (Files.exists(path)) {
            return true;
        }
        try {
            Files.createDirectories(path);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean createFolderForFile(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }

        Path file = Paths.get(filePath);
        Path parent = file.getParent(); // Lấy thư mục cha

        if (parent == null) {
            // Trường hợp không có thư mục cha (ví dụ chỉ là "file.txt")
            return true;
        }

        if (Files.exists(parent)) {
            return true;
        }

        try {
            Files.createDirectories(parent);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static boolean clearFolder(String folderPath) {
        Path folder = Paths.get(folderPath);
        if (!Files.exists(folder) || !Files.isDirectory(folder)) {
            return false;
        }
        try {
            Files.walk(folder)
                    .skip(1)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
            return true;

        } catch (IOException e) {
            return false;
        }
    }

    public static String sanitizeFileName(String input) {
        if (input == null || input.isEmpty()) {
            return "untitled";
        }
        String sanitized = input.replaceAll("[\\\\/:*?\"<>|]", "");
        sanitized = sanitized.replaceAll("[\\p{Cntrl}]", "");
        sanitized = sanitized.trim();
        if (sanitized.isEmpty()) {
            sanitized = "untitled";
        }

        return sanitized;
    }
}
