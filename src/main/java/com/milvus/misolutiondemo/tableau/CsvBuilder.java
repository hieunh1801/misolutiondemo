package com.milvus.misolutiondemo.tableau;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvBuilder {
    private static final SecureRandom random = new SecureRandom();

    public static String randomAsciiString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int code = 32 + random.nextInt(95); // từ 32 (space) đến 126 (~)
            sb.append((char) code);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        List<Map<String, Object>> records = createMockData(100);
        String outputFilePath = "./output/csv/product.csv";
        writeMapsToCsv(outputFilePath, records, 1000);
    }

    public static List<Map<String, Object>> createMockData(int count) {
        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("id", i);
            record.put("name", "Product " + i);
            record.put("price", randomAsciiString(10));
            record.put("quantity", randomAsciiString(20));
            record.put("unitPrice", randomAsciiString(30));
            record.put("stock", randomAsciiString(40));
            record.put("image", randomAsciiString(30));
            record.put("category", randomAsciiString(20));
            record.put("model", randomAsciiString(30));
            record.put("manufacturer", randomAsciiString(20));
            records.add(record);
        }
        return records;
    }

    public static void writeMapsToCsv(String filePath, List<Map<String, Object>> records, int batchSize) {
        if (records == null || records.isEmpty()) {
            return;
        }
        String[] header = records.get(0).keySet().toArray(new String[0]);
        FileHelper.createFolderForFile(filePath);
        File file = new File(filePath);
        // create folder if not existed

        boolean append = file.exists();

        try (
                FileOutputStream fos = new FileOutputStream(file, append);
                OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                CSVWriter writer = new CSVWriter(osw)
        ) {
            if (!append) {
                writer.writeNext(header); // chỉ ghi header lần đầu
            }

            int total = records.size();
            for (int start = 0; start < total; start += batchSize) {
                int end = Math.min(start + batchSize, total);
                List<Map<String, Object>> batch = records.subList(start, end);

                for (Map<String, Object> record : batch) {
                    String[] line = new String[header.length];
                    for (int i = 0; i < header.length; i++) {
                        Object value = record.get(header[i]);
                        line[i] = (value != null) ? value.toString() : "";
                    }
                    writer.writeNext(line);
                }

                writer.flush();
                log.info("✅ Write: {} ({} lines)", start / batchSize + 1, batch.size());
            }

            log.info("\uD83C\uDF89 write {} success {}", total, filePath);
        } catch (IOException e) {
            log.info("❌ Error {}", e.getMessage());
        }
    }

}
