package com.milvus.misolutiondemo.tableau;

import com.milvus.misolutiondemo.common.SystemUtil;
import com.tableau.hyperapi.*;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.*;

import static com.tableau.hyperapi.Nullability.NOT_NULLABLE;

@Slf4j
public class HyperBuilder {
    private final int HYPER_BATCH_SIZE = 1000;

    public static List<Map<String, Object>> csvStringToList(String csvString) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        if (csvString == null || csvString.isBlank()) return result;
        try (BufferedReader br = new BufferedReader(new StringReader(csvString))) {
            String headerLine = br.readLine();
            if (headerLine == null) return result;

            String[] headers = headerLine.split(",", -1);

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1);
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String key = headers[i].trim();
                    String value = i < values.length ? values[i].trim() : "";
                    row.put(key, value);
                }
                result.add(row);
            }
        }
        return result;
    }

    public void build(String tableName, String csvContent) throws Exception {
        List<Map<String, Object>> data = csvStringToList(csvContent);
        Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, Object> row : data) {
            allKeys.addAll(row.keySet());
        }
        String outputFilePath = String.format("./output/hyper/%s.hyper", tableName);
        TableDefinition tableDefinition = getTableDefinition(allKeys, tableName, tableName);

        try (HyperProcess process = getHyperProcess()) {
            try (Connection connection = new Connection(
                    process.getEndpoint(),
                    outputFilePath,
                    CreateMode.CREATE_IF_NOT_EXISTS)) {
                Catalog catalog = connection.getCatalog();
                SchemaName schemaName = new SchemaName(tableName);
                if (!catalog.getSchemaNames().contains(schemaName)) {
                    catalog.createSchema(schemaName);
                }

                TableName tableNameObj = new TableName(tableName, tableDefinition.getTableName().getName());
                if (!catalog.getTableNames(schemaName).contains(tableNameObj)) {
                    catalog.createTable(tableDefinition);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


            // write hyper file
            try (Connection connection = new Connection(
                    process.getEndpoint(),
                    outputFilePath,
                    CreateMode.CREATE_IF_NOT_EXISTS)) {

                Catalog catalog = connection.getCatalog();
                SchemaName schemaName = new SchemaName(tableName);
                if (!catalog.getSchemaNames().contains(schemaName)) {
                    catalog.createSchema(schemaName);
                }

                TableName tableNameObj = new TableName(tableName, tableDefinition.getTableName().getName());
                if (!catalog.getTableNames(schemaName).contains(tableNameObj)) {
                    catalog.createTable(tableDefinition);
                }

                List<Map<String, Object>> batch = new ArrayList<>();
                for (Map<String, Object> row : data) {
                    batch.add(row);
                    if (batch.size() >= HYPER_BATCH_SIZE) {
                        insertBatchIntoHyper(connection, tableDefinition, allKeys, batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    insertBatchIntoHyper(connection, tableDefinition, allKeys, batch);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertBatchIntoHyper(Connection connection, TableDefinition tableDefinition,
                                      Set<String> allKeys, List<Map<String, Object>> batch) {
        try (Inserter inserter = new Inserter(connection, tableDefinition)) {
            batch.forEach(row -> {
                allKeys.forEach(key -> {
                    String value = row.containsKey(key) && row.get(key) != null ? row.get(key).toString() : "";
                    inserter.add(value);
                });
                inserter.endRow();
            });
            inserter.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private TableDefinition getTableDefinition(Set<String> keys,
                                               String schemaName,
                                               String tableName) {
        TableDefinition tableDefinition = new TableDefinition(
                new TableName(schemaName, tableName));
        keys.forEach(key -> {
            tableDefinition.addColumn(key, SqlType.text(), NOT_NULLABLE);
        });
        return tableDefinition;
    }

    private HyperProcess getHyperProcess() {
        String userDir = System.getProperty("user.dir");
        String hyperExecPath = "/lib_windows/hyper";
        switch (SystemUtil.getOS()) {
            case LINUX:
                hyperExecPath = "/lib_linux/hyper";
                break;
            case SOLARIS:
                hyperExecPath = "/lib_solaris/hyper";
                break;
            case MACOS:
                hyperExecPath = "/lib_macos/hyper";
                break;
        }
        return new HyperProcess(Paths.get(userDir + hyperExecPath),
                Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);
    }

    public static void main(String[] args) throws Exception {
        log.info("TEST HYPER BUILDER");
        demoOrder();
        demoOrderDetail();
    }

    private static void demoOrder() throws Exception {
        String tableName = "order";
        String csvString = """
                order_id,customer_name,order_date,total_amount
                O001,Bob,2025-11-05,250.00
                O002,Hiếu,2025-11-05,180.50
                O003,Jane,2025-11-05,99.99
                O004,Chris,2025-11-05,320.00
                O005,Anna,2025-11-05,155.75
                O006,Tom,2025-11-05,420.20
                O007,Lisa,2025-11-05,510.00
                O008,David,2025-11-05,270.30
                O009,Kate,2025-11-05,88.00
                O010,John,2025-11-05,150.60
                """;
        HyperBuilder hyperBuilder = new HyperBuilder();
        hyperBuilder.build(tableName, csvString);
    }

    private static void demoOrderDetail() throws Exception {
        String tableName = "order_detail";
        String csvString = """
                detail_id,order_id,product_name,quantity,price
                D001,O001,Keyboard,2,50.00
                D002,O001,Mouse,1,30.00
                D003,O002,Monitor,1,180.50
                D004,O003,USB Cable,3,9.99
                D005,O004,Laptop,1,320.00
                D006,O005,Charger,2,77.88
                D007,O006,Headphones,2,210.10
                D008,O007,Webcam,1,110.00
                D009,O008,SSD Drive,1,270.30
                D010,O009,Pen Drive,4,22.00
                """;
        HyperBuilder hyperBuilder = new HyperBuilder();
        hyperBuilder.build(tableName, csvString);
    }
}
