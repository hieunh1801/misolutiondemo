package com.milvus.misolutiondemo.tableau;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

@Data
@Slf4j
public class TdsxBuilder {
    private final String id;
    private TdsBuilder tdsBuilder;
    private TdsMetadata tdsMetadata;
    private String outputFolder; // relative path
    private String folder; // outputFolder + fileName
    private String extractFolder; // outputFolder/Data/Extracts;
    private String dataFolder; // outputFolder/Data

    private String tdsxFileName; // name
    private String fileExtension; // csv | hyper

    private Map<String, String> dataNameMapper; // data file name
    private Map<String, String> dataPathMapper; // data file path

    public TdsxBuilder(String tdsxFileName, String outputFolder) {
        this.id = UUID.randomUUID().toString();
        this.outputFolder = outputFolder;
        this.tdsxFileName = tdsxFileName;
        this.fileExtension = ".tdsx";
        this.folder = outputFolder + "/" + tdsxFileName;
        this.extractFolder = outputFolder + "/" + tdsxFileName + "/Data/Extracts";
        this.dataFolder = outputFolder + "/" + tdsxFileName + "/Data";

        this.tdsMetadata = new TdsMetadata();
        this.tdsBuilder = new TdsBuilder(this.tdsMetadata);
        this.dataNameMapper = new HashMap<>();
        this.dataPathMapper = new HashMap<>();
        FileHelper.clearFolder(outputFolder);
        FileHelper.createFolder(folder);
//        FileHelper.createFolder(extractFolder);
        FileHelper.createFolder(dataFolder);
    }

//    public TdsMetadata.TdsTable createHyper(String tableName, String csvContent) throws Exception {
//        String hyperFileName = fileMapper.get(tableName);
//        if (hyperFileName == null) {
//            hyperFileName = TableauUtil.generateRandom(32);
//            fileMapper.put(tableName, hyperFileName);
//        }
//        HyperBuilder hyperBuilder = new HyperBuilder();
//        hyperBuilder.build(hyperFileName, extractFolder, tableName, csvContent);
//
//        TdsMetadata.TdsConnection tdsConnection = TdsMetadata.TdsConnection.createHyperConnection(tableName, hyperFileName);
//        tdsMetadata.addTdsConnection(tdsConnection);
//
//        TdsMetadata.TdsTable tdsTable = new TdsMetadata.TdsTable(tableName + "." + tableName);
//        return tdsMetadata.addTdsTable(tdsConnection, tdsTable);
//    }

    public TdsMetadata.TdsTable createCsv(String tableName, List<Map<String, Object>> records) {
        String csvFileName = dataNameMapper.get(tableName);
        String csvFilePath = dataPathMapper.get(tableName);
        if (csvFileName == null) {
            csvFileName = FileHelper.sanitizeFileName(tableName);
            csvFilePath = TableauUtil.generateRandom(20);
            dataNameMapper.put(tableName, csvFileName);
            dataPathMapper.put(tableName, csvFilePath);
        }
        String filePath = String.format("%s/%s/%s.csv", dataFolder, csvFilePath, csvFileName);
        log.info("WRITE CSV {}", filePath);
        CsvBuilder.writeMapsToCsv(filePath, records, 1000);
        TdsMetadata.TdsConnection tdsConnection = TdsMetadata.TdsConnection.createCsvConnection(tableName, csvFileName);
        tdsMetadata.addTdsConnection(tdsConnection);

        TdsMetadata.TdsTable tdsTable = new TdsMetadata.TdsTable(tableName + "." + tableName);
        tdsMetadata.addTdsTable(tdsConnection, tdsTable);
        tdsTable.setType("csv");
        return tdsTable;
    }

    public TdsxBuilder build() throws Exception {
        this.tdsBuilder
                .build()
                .writeToFile(folder, tdsxFileName);
        return this;
    }

    public TdsxBuilder zip() throws IOException {
        TableauUtil.zipFolder(folder, outputFolder + "/" + tdsxFileName + ".tdsx");
        return this;
    }


    public static void main(String[] args) throws Exception {
        String fileName = "mix001_1data";
        String outputFolder = "./output/tdsx";
        TdsxBuilder tdsxBuilder = new TdsxBuilder(fileName, outputFolder);
        TdsMetadata tdsMetadata = tdsxBuilder.getTdsMetadata();
        TdsMetadata.TableauInfo tableauInfo = new TdsMetadata.TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
//        TdsMetadata.TdsConnection conn1 = TdsMetadata.TdsConnection.createConnection(
//                "postgres",
//                "3.35.93.207",
//                "5432",
//                "misolution_dev",
//                "postgres",
//                "Mlv_IT#25A"
//        );
//        tdsMetadata.addTdsConnection(conn1);
//
//        TdsMetadata.TdsTable con1tbl1 = new TdsMetadata.TdsTable("public.test");
//        tdsMetadata.addTdsTable(conn1, con1tbl1);
//
//        TdsMetadata.TdsColumn conn1tbl1col = new TdsMetadata.TdsColumn("name");
//        tdsMetadata.addTdsColumn(con1tbl1, conn1tbl1col);

        List<Map<String, Object>> records = CsvBuilder.createMockData(10);
        TdsMetadata.TdsTable con2tbl2 = tdsxBuilder.createCsv("products", records);
        TdsMetadata.TdsColumn conn2tbl2col = new TdsMetadata.TdsColumn("id");
        tdsMetadata.addTdsColumn(con2tbl2, conn2tbl2col);

//        tdsxBuilder.build().zip();
        tdsxBuilder.build();
        log.info("> tabcmd publish {}/{}.tdsx --project='ts_tds_builder' --overwrite ", outputFolder, fileName);
    }
}
