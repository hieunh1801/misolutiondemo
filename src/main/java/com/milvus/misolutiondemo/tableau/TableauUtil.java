package com.milvus.misolutiondemo.tableau;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.io.*;
import java.nio.file.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TableauUtil {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static String generateRandom(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("length > 0");
        }
        SecureRandom random = new SecureRandom();
        return IntStream.range(0, length).map(i -> {
            int randomIndex = random.nextInt(CHARACTERS.length());
            return CHARACTERS.charAt(randomIndex);
        }).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    public static Comparator<String> createNameComparator() {
        return (a, b) -> {
            int numA = extractNumber(a);
            int numB = extractNumber(b);

            if (numA == -1 && numB == -1) return a.compareTo(b);

            if (numA == -1) return -1;
            if (numB == -1) return 1;

            return Integer.compare(numB, numA);
        };
    }

    private static int extractNumber(String s) {
        String num = s.replaceAll("\\D+", ""); // Lấy phần số
        return num.isEmpty() ? -1 : Integer.parseInt(num);
    }

    public static void main(String[] args) {
        demoNameComparator();
    }

    private static void demoNameComparator() {
        List<String> list = Arrays.asList("test", "test1", "test2", "test3");
        List<String> list1 = Arrays.asList("test1", "test11", "test12", "test13");
        list.sort(createNameComparator());
        list1.sort(createNameComparator());
        System.out.println(list);
        System.out.println(list1);
    }

//    public static void zipFolder(String sourceDirPath, String zipFilePath) throws IOException {
//        Path sourceDir = Paths.get(sourceDirPath);
//        Path zipFile = Paths.get(zipFilePath);
//
//        if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
//            throw new IOException("target folder is not existed or not an directory " + sourceDirPath);
//        }
//
//        try (OutputStream fos = Files.newOutputStream(zipFile);
//             ZipOutputStream zipOut = new ZipOutputStream(fos)) {
//
//            try (Stream<Path> stream = Files.walk(sourceDir)) {
//                stream.filter(path -> !Files.isDirectory(path)) // Chỉ xử lý các file
//                        .forEach(file -> {
//                            try {
//                                // Gọi hàm phụ trợ để nén từng file
//                                zipFile(sourceDir, file, zipOut);
//                            } catch (IOException e) {
//                                // Xử lý lỗi nếu có vấn đề với một file cụ thể
//                                System.err.println("Lỗi khi nén file " + file + ": " + e.getMessage());
//                            }
//                        });
//            }
//        }
//        System.out.println("Zip success: " + sourceDirPath + " -> " + zipFilePath);
//    }
//
//    private static void zipFile(Path sourceDir, Path fileToZip, ZipOutputStream zipOut) throws IOException {
//        String zipEntryName = sourceDir.relativize(fileToZip).toString();
//        ZipEntry zipEntry = new ZipEntry(zipEntryName);
//        zipOut.putNextEntry(zipEntry);
//        Files.copy(fileToZip, zipOut);
//        zipOut.closeEntry();
//    }
//
//    public static void zipFolder(String sourceDirPath, String outputFilePath) throws IOException {
//        Path sourceDir = Paths.get(sourceDirPath);
//        try (
//                OutputStream fOut = Files.newOutputStream(Paths.get(outputFilePath));
//                BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
//                GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
//                TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)
//        ) {
//            Files.walk(sourceDir)
//                    .filter(path -> !Files.isDirectory(path))
//                    .forEach(path -> {
//                        TarArchiveEntry entry = new TarArchiveEntry(sourceDir.relativize(path).toString());
//                        try {
//                            entry.setSize(Files.size(path));
//                            tOut.putArchiveEntry(entry);
//                            Files.copy(path, tOut);
//                            tOut.closeArchiveEntry();
//                        } catch (IOException e) {
//                            throw new UncheckedIOException(e);
//                        }
//                    });
//            tOut.finish();
//        }
//    }

    public static void zipFolder(String sourceDirPath, String outputFilePath) throws IOException {
        Path sourceDir = Paths.get(sourceDirPath);
        try (
                OutputStream fos = Files.newOutputStream(Paths.get(outputFilePath));
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                ZipOutputStream zos = new ZipOutputStream(bos)
        ) {
            Files.walk(sourceDir)
                    .filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        // Lấy relative path để không bao gồm folder cha
                        Path relativePath = sourceDir.relativize(path);
                        ZipEntry zipEntry = new ZipEntry(relativePath.toString().replace("\\", "/"));
                        try {
                            zos.putNextEntry(zipEntry);
                            Files.copy(path, zos);
                            zos.closeEntry();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
