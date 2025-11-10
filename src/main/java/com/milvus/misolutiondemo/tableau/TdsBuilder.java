package com.milvus.misolutiondemo.tableau;

import lombok.extern.slf4j.Slf4j;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.milvus.misolutiondemo.tableau.TdsMetadata.*;

@Slf4j
public class TdsBuilder {
    private final Document document;
    private final TdsMetadata tdsMetadata;
    private Element datasource;
    private Element manifest;
    private Element repositoryLocation;

    public TdsBuilder(TdsMetadata tdsMetadata) {
        this.tdsMetadata = tdsMetadata;
        this.document = DocumentHelper.createDocument();

        if (this.tdsMetadata == null) {
            throw new IllegalArgumentException("tds is null");
        }
    }

    private TdsBuilder addDatasource() {
        final String version = "18.1";
        final String formattedName = "federated." + TableauUtil.generateRandom(28);
        this.datasource = document
                .addElement("datasource")
                .addAttribute("formattedName", formattedName)
//                .addAttribute("inline", "true")
//                .addAttribute("source-platform", "linux")
                .addAttribute("version", version)
//                .addAttribute("xml:base", tdsMetadata.getTableauInfo().getHost())
//                .addAttribute("xmlns:user", "http://www.tableausoftware.com/xml/user")
        ;
        return this;
    }

    private TdsBuilder addManifest() {
        manifest = datasource.addElement("document-format-change-manifest");
        manifest.addElement("_.fcp.ObjectModelEncapsulateLegacy.true...ObjectModelEncapsulateLegacy");
//        if (!tdsMetadata.getTdsRelationships().isEmpty()) {
//            manifest.addElement("_.fcp.ObjectModelRelationshipPerfOptions.true...ObjectModelRelationshipPerfOptions");
//        }
        manifest.addElement("_.fcp.ObjectModelTableType.true...ObjectModelTableType");
        manifest.addElement("_.fcp.SchemaViewerObjectModel.true...SchemaViewerObjectModel");
        return this;
    }

    private TdsBuilder addRepositoryLocation() {
        String id = TableauUtil.generateRandom(28);
        String site = tdsMetadata.getTableauInfo().getSite();
        String path = String.format("/t/%s/datasources", site);
        repositoryLocation = datasource.addElement("repository-location")
                .addAttribute("id", id)
                .addAttribute("path", path)
                .addAttribute("revision", "1.0")
                .addAttribute("site", site);
        return this;
    }

    private TdsBuilder addConnection() {
        List<TdsConnection> connections = tdsMetadata.getTdsConnections();
        Element connectionEl = datasource.addElement("connection").addAttribute("class", "federated");

        // add namedConnection
        Element namedConnectionsEl = connectionEl.addElement("named-connections");
        for (TdsConnection connection : connections) {
            Element namedConnectionEl = namedConnectionsEl
                    .addElement("named-connection")
                    .addAttribute("caption", connection.getNamedConnectionCaption())
                    .addAttribute("name", connection.getNamedConnectionName());

            if ("data".equals(connection.getType())) {
                namedConnectionEl.addElement("connection")
                        .addAttribute("class", connection.getConnectionClass())
                        .addAttribute("directory", connection.getConnectionDirectory())
                        .addAttribute("filename", connection.getConnectionFilename());
            } else {
                namedConnectionEl
                        .addElement("connection")
                        .addAttribute("class", connection.getConnectionClass())
                        .addAttribute("authentication", connection.getConnectionAuthentication())
                        .addAttribute("dbname", connection.getConnectionDbName())
                        .addAttribute("one-time-sql", connection.getConnectionOneTimeSql())
                        .addAttribute("port", connection.getConnectionPort())
                        .addAttribute("server", connection.getConnectionServer())
                        .addAttribute("username", connection.getConnectionUsername())
                        .addAttribute("workgroup-auth-mode", connection.getConnectionWorkgroupAuthMode());
            }
        }

        // add relation
//        Element objectModelEncapsulateLegacyFalseEl = connectionEl.addElement("_.fcp.ObjectModelEncapsulateLegacy.false...relation").addAttribute("type", "collection");
        Element objectModelEncapsulateLegacyTrueEl = connectionEl.addElement("_.fcp.ObjectModelEncapsulateLegacy.true...relation").addAttribute("type", "collection");

        List<TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsTable tdsTable : tdsTables) {
            TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsTable.getTdsConnectionId());
            objectModelEncapsulateLegacyTrueEl
                    .addElement("_.fcp.ObjectModelSharedDimensions.true...relation")
                    .addAttribute("connection", tdsConnection.getNamedConnectionName())
                    .addAttribute("name", tdsTable.getName())
                    .addAttribute("table", tdsTable.getTable())
                    .addAttribute("type", "table");
        }

        // add cols <map> // this is column mapper
        Element colEl = connectionEl.addElement("cols");
        List<TdsColumn> tdsColumns = tdsMetadata.getTdsColumns();
        for (TdsColumn tdsColumn : tdsColumns) {
            TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsColumn.getTdsConnectionId());
            TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());

            String key = String.format("[%s]", tdsColumn.getName());
            String value = String.format("[%s].[%s]", tdsTable.getName(), tdsColumn.getOriginalName());
            colEl.addElement("map")
                    .addAttribute("key", key)
                    .addAttribute("value", value);
        }

        // add metadata-records // this is column information
//        Element metadataRecordsEl = connectionEl.addElement("metadata-records");
//        for (TdsColumn tdsColumn : tdsColumns) {
//            TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsColumn.getTdsConnectionId());
//            TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());
//
//            Element metadataRecordEl = metadataRecordsEl.addElement("_.fcp.ObjectModelSharedDimensions.true...metadata-record")
//                    .addAttribute("class", "column");
//            metadataRecordEl.addElement("local-name")
//                    .setText(String.format("[%s]", tdsColumn.getName()));
//            metadataRecordEl.addElement("parent-name")
//                    .setText(String.format("[%s]", tdsTable.getName()));
//            metadataRecordEl.addElement("_.fcp.ObjectModelEncapsulateLegacy.true...object-id")
//                    .setText(String.format("[%s]", tdsTable.getObjectId()));
//        }

        return this;
    }

    private TdsBuilder addAliases() {
        datasource.addElement("aliases").addAttribute("enabled", "yes");
        return this;
    }

    private TdsBuilder addColumn() {
        // add _.fcp.ObjectModelTableType.true...column
        List<TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsTable table : tdsTables) {
            datasource.addElement("_.fcp.ObjectModelTableType.true...column")
                    .addAttribute("caption", table.getName())
                    .addAttribute("datatype", "table")
                    .addAttribute("name", String.format("[__tableau_internal_object_id__].[%s]", table.getObjectId()))
                    .addAttribute("role", "measure")
                    .addAttribute("type", "quantitative");
        }

        // add column
        List<TdsColumn> tdsColumns = tdsMetadata.getTdsColumns();
        for (TdsColumn column : tdsColumns) {
            datasource.addElement("column")
                    .addAttribute("caption", column.getName())
                    .addAttribute("name", String.format("[%s]", column.getName()));
        }

        datasource.addElement("layout")
                .addAttribute("_.fcp.SchemaViewerObjectModel.false...dim-percentage", "0.5")
                .addAttribute("_.fcp.SchemaViewerObjectModel.false...measure-percentage", "0.4")
                .addAttribute("dim-ordering", "alphabetic")
                .addAttribute("measure-ordering", "alphabetic")
                .addAttribute("show-structure", "true");
        return this;
    }


    private TdsBuilder addObjectGraph() {
        Element objectGraphEl = datasource.addElement("_.fcp.ObjectModelEncapsulateLegacy.true...object-graph");

        // ADD OBJECTS: each object is a table
        Element objectsEl = objectGraphEl.addElement("objects");
        List<TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsTable table : tdsTables) {
            TdsConnection tdsConnection = tdsMetadata.getTdsConnection(table.getTdsConnectionId());
            Element objectEl = objectsEl.addElement("object");
            objectEl.addAttribute("caption", table.getName());
            objectEl.addAttribute("id", table.getObjectId());

            Element properties = objectEl.addElement("properties");
            properties.addAttribute("context", "");

            Element relation = properties.addElement("relation");
            relation.addAttribute("connection", tdsConnection.getNamedConnectionName())
                    .addAttribute("name", table.getName())
                    .addAttribute("table", table.getTable())
                    .addAttribute("type", "table");

        }
        // ADD RELATIONSHIPS
        Element relationshipsEl = objectGraphEl.addElement("relationships");
        List<TdsRelationship> tdsRelationships = tdsMetadata.getTdsRelationships();
        for (TdsRelationship tdsRelationship : tdsRelationships) {
            TdsColumn col1 = tdsRelationship.getCol1();
            TdsColumn col2 = tdsRelationship.getCol2();
            TdsTable tbl1 = tdsMetadata.getTdsTable(col1.getTdsTableId());
            TdsTable tbl2 = tdsMetadata.getTdsTable(col2.getTdsTableId());

            String operator = tdsRelationship.getOperator();

            Element relationshipEl = relationshipsEl.addElement("relationship");
            Element expressionEl = relationshipEl.addElement("expression")
                    .addAttribute("op", operator);
            expressionEl.addElement("expression")
                    .addAttribute("op", String.format("[%s]", col1.getName()));
            expressionEl.addElement("expression")
                    .addAttribute("op", String.format("[%s]", col2.getName()));
            relationshipEl.addElement("first-end-point")
                    .addAttribute("object-id", tbl1.getObjectId());
            relationshipEl.addElement("second-end-point")
                    .addAttribute("object-id", tbl2.getObjectId());
        }
        return this;
    }

    public TdsBuilder build() throws Exception {
        tdsMetadata.compute();

        this.printInfo();

        this.addDatasource()
                .addManifest()
                .addRepositoryLocation()
                .addConnection()
//                .addAliases()
//                .addColumn()
                .addObjectGraph();
        return this;
    }

    private void printInfo() {
//        for (TdsConnection tdsConnection : tdsMetadata.getTdsConnections()) {
//            log.info("CONNECTION => {}", tdsConnection);
//        }
//        for (TdsTable tdsTable : tdsMetadata.getTdsTables()) {
//            log.info("TABLE => {} {}", tdsTable.getObjectId(), tdsTable.getName());
//        }

        for (TdsColumn tdsColumn : tdsMetadata.getTdsColumns()) {
            TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());
            log.info("TABLE => [{}] [{}] => COLUMN [{}]", tdsTable.getObjectId(), tdsTable.getName(), tdsColumn.getName());
        }

        for (TdsRelationship tdsRelationship : tdsMetadata.getTdsRelationships()) {
            TdsColumn col1 = tdsRelationship.getCol1();
            TdsColumn col2 = tdsRelationship.getCol2();

            TdsTable tbl1 = tdsMetadata.getTdsTable(col1.getTdsTableId());
            TdsTable tbl2 = tdsMetadata.getTdsTable(col2.getTdsTableId());
        }
//
//        for (TdsRelationship tdsRelationship: tdsMetadata.getTdsRelationships()) {
//            log.info("RELATIONSHIP => {}", tdsRelationship);
//        }
    }

    public void writeToFile(String outputFolder, String fileName) throws Exception {
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("UTF-8");
        String outputFile = outputFolder + "/" + fileName + ".tds";
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8)) {
            XMLWriter xmlWriter = new XMLWriter(writer, format);
            xmlWriter.write(document);
            xmlWriter.close();
            log.info("XML file built successfully: {}", fileName);
        }
    }

}
