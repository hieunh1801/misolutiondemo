package com.milvus.misolutiondemo.tableau;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.milvus.misolutiondemo.tableau.TdsMetadata.*;

@Data
@Slf4j
public class TdsxBuilder {
    private final String id;
    private TdsBuilder tdsBuilder;
    private TdsMetadata tdsMetadata;
    private String outputFolder; // relative path
    private String tdsxFolder; // outputFolder + fileName
//    private String extractFolder = "Data/Extracts"; // outputFolder/Data/Extracts;
//    private String dataFolder = "Data"; // /Data

    private String tdsxFileName; // name
    private String tdsxFileExtension = "tdsx"; // csv | hyper

    private Map<String, String> dataNameMapper; // data file name
    private Map<String, String> dataPathMapper; // data file path

    public TdsxBuilder(String tdsxFileName, String outputFolder) {
        this.id = UUID.randomUUID().toString();
        this.outputFolder = outputFolder;
        this.tdsxFileName = tdsxFileName;
        this.tdsxFolder = outputFolder + "/" + tdsxFileName;

        this.tdsMetadata = new TdsMetadata();
        this.tdsBuilder = new TdsBuilder(this.tdsMetadata);
        this.dataNameMapper = new HashMap<>();
        this.dataPathMapper = new HashMap<>();
        FileHelper.clearFolder(tdsxFolder);
        FileHelper.createFolder(tdsxFolder);
    }

    public TdsTable createCsv(String tableName, List<Map<String, Object>> records) {
        String csvFileName = FileHelper.sanitizeFileName(tableName);
        String csvFilePath = TableauUtil.generateRandom(20);
        String csvFileExtension = "csv";
        String filePath = Path.of(tdsxFolder, "Data", csvFilePath, csvFileName + "." + csvFileExtension).toString();
        log.info("WRITE CSV {}", filePath);
        CsvBuilder.writeMapsToCsv(filePath, records, 1000);
        String connectionDirectory = "Data/" + csvFilePath;
        String connectionFilename = csvFileName + "." + csvFileExtension;
        TdsConnection tdsConnection = TdsConnection.createCsvConnection(connectionDirectory, connectionFilename);
        tdsMetadata.addTdsConnection(tdsConnection);

        TdsTable tdsTable = TdsTable.createCsvTable(tableName, csvFileName);
        tdsMetadata.addTdsTable(tdsConnection, tdsTable);
        tdsTable.setType("csv");
        return tdsTable;
    }


    public TdsxBuilder build() throws Exception {
        this.tdsBuilder
                .build()
                .writeToFile(tdsxFolder, tdsxFileName);
        return this;
    }

    public TdsxBuilder zip() throws IOException {
        TableauUtil.zipFolder(tdsxFolder, outputFolder + "/" + tdsxFileName + "_builder.tdsx");
        System.out.println(String.format("> tabcmd publish %s/%s_builder.tdsx --project='ts_tds_builder' --overwrite ", outputFolder, tdsxFileName));
        System.out.println(String.format("> unzip %s/%s_builder.tdsx -d %s/%s_builder", outputFolder, tdsxFileName, outputFolder, tdsxFileName));

        return this;
    }
}
