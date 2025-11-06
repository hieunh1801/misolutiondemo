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
                .addAttribute("inline", "true")
                .addAttribute("source-platform", "linux")
                .addAttribute("version", version)
                .addAttribute("xml:base", tdsMetadata.getTableauInfo().getHost())
                .addAttribute("xmlns:user", "http://www.tableausoftware.com/xml/user");
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
        List<TdsMetadata.TdsConnection> connections = tdsMetadata.getTdsConnections();
        Element connectionEl = datasource.addElement("connection").addAttribute("class", "federated");

        // add namedConnection
        Element namedConnectionsEl = connectionEl.addElement("named-connections");
        for (TdsMetadata.TdsConnection connection : connections) {
            Element namedConnectionEl = namedConnectionsEl
                    .addElement("named-connection")
                    .addAttribute("caption", connection.getHost())
                    .addAttribute("name", connection.getNamedConnection());

            namedConnectionEl
                    .addElement("connection")
                    .addAttribute("authentication", "username-password")
                    .addAttribute("class", connection.getConnectionType().getName())
                    .addAttribute("dbname", connection.getDatabase())
                    .addAttribute("one-time-sql", "")
                    .addAttribute("port", connection.getPort())
                    .addAttribute("server", connection.getHost())
                    .addAttribute("username", connection.getUsername())
                    .addAttribute("workgroup-auth-mode", "");
        }

        // add relation
//        Element objectModelEncapsulateLegacyFalseEl = connectionEl.addElement("_.fcp.ObjectModelEncapsulateLegacy.false...relation").addAttribute("type", "collection");
        Element objectModelEncapsulateLegacyTrueEl = connectionEl.addElement("_.fcp.ObjectModelEncapsulateLegacy.true...relation").addAttribute("type", "collection");

        List<TdsMetadata.TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsMetadata.TdsTable tdsTable : tdsTables) {
            TdsMetadata.TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsTable.getTdsConnectionId());
//                if (table.isRoot) {
//                    objectModelEncapsulateLegacyFalseEl
//                            .addAttribute("connection", connection.namedConnection)
//                            .addAttribute("name", table.name)
//                            .addAttribute("table", table.table)
//                            .addAttribute("type", "table");
//                }

            objectModelEncapsulateLegacyTrueEl
                    .addElement("_.fcp.ObjectModelSharedDimensions.true...relation")
                    .addAttribute("connection", tdsConnection.getNamedConnection())
                    .addAttribute("name", tdsTable.getName())
                    .addAttribute("table", tdsTable.getTable())
                    .addAttribute("type", "table");
        }

        // add cols <map> // this is column mapper
        Element colEl = connectionEl.addElement("cols");
        List<TdsMetadata.TdsColumn> tdsColumns = tdsMetadata.getTdsColumns();
        for (TdsMetadata.TdsColumn tdsColumn : tdsColumns) {
            TdsMetadata.TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsColumn.getTdsConnectionId());
            TdsMetadata.TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());

            String key = String.format("[%s]", tdsColumn.getName());
            String value = String.format("[%s].[%s]", tdsTable.getName(), tdsColumn.getOriginalName());
            colEl.addElement("map")
                    .addAttribute("key", key)
                    .addAttribute("value", value);
        }

        // add metadata-records // this is column information
//        Element metadataRecordsEl = connectionEl.addElement("metadata-records");
//        for (TdsMetadata.TdsColumn tdsColumn : tdsColumns) {
//            TdsMetadata.TdsConnection tdsConnection = tdsMetadata.getTdsConnection(tdsColumn.getTdsConnectionId());
//            TdsMetadata.TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());
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
        List<TdsMetadata.TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsMetadata.TdsTable table : tdsTables) {
            datasource.addElement("_.fcp.ObjectModelTableType.true...column")
                    .addAttribute("caption", table.getName())
                    .addAttribute("datatype", "table")
                    .addAttribute("name", String.format("[__tableau_internal_object_id__].[%s]", table.getObjectId()))
                    .addAttribute("role", "measure")
                    .addAttribute("type", "quantitative");
        }

        // add column
        List<TdsMetadata.TdsColumn> tdsColumns = tdsMetadata.getTdsColumns();
        for (TdsMetadata.TdsColumn column : tdsColumns) {
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
        List<TdsMetadata.TdsTable> tdsTables = tdsMetadata.getTdsTables();
        for (TdsMetadata.TdsTable table : tdsTables) {
            TdsMetadata.TdsConnection tdsConnection = tdsMetadata.getTdsConnection(table.getTdsConnectionId());
            Element objectEl = objectsEl.addElement("object");
            objectEl.addAttribute("caption", table.getName());
            objectEl.addAttribute("id", table.getObjectId());

            Element properties = objectEl.addElement("properties");
            properties.addAttribute("context", "");

            Element relation = properties.addElement("relation");
            relation.addAttribute("connection", tdsConnection.getNamedConnection())
                    .addAttribute("name", table.getName())
                    .addAttribute("table", table.getTable())
                    .addAttribute("type", "table");

        }
        // ADD RELATIONSHIPS
        Element relationshipsEl = objectGraphEl.addElement("relationships");
        List<TdsMetadata.TdsRelationship> tdsRelationships = tdsMetadata.getTdsRelationships();
        for (TdsMetadata.TdsRelationship tdsRelationship : tdsRelationships) {
            TdsMetadata.TdsColumn col1 = tdsRelationship.getCol1();
            TdsMetadata.TdsColumn col2 = tdsRelationship.getCol2();
            TdsMetadata.TdsTable tbl1 = tdsMetadata.getTdsTable(col1.getTdsTableId());
            TdsMetadata.TdsTable tbl2 = tdsMetadata.getTdsTable(col2.getTdsTableId());

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
//        for (TdsMetadata.TdsConnection tdsConnection : tdsMetadata.getTdsConnections()) {
//            log.info("CONNECTION => {}", tdsConnection);
//        }
//        for (TdsMetadata.TdsTable tdsTable : tdsMetadata.getTdsTables()) {
//            log.info("TABLE => {} {}", tdsTable.getObjectId(), tdsTable.getName());
//        }

        for (TdsMetadata.TdsColumn tdsColumn : tdsMetadata.getTdsColumns()) {
            TdsMetadata.TdsTable tdsTable = tdsMetadata.getTdsTable(tdsColumn.getTdsTableId());
            log.info("TABLE => [{}] [{}] => COLUMN [{}]", tdsTable.getObjectId(), tdsTable.getName(), tdsColumn.getName());
        }

        for (TdsMetadata.TdsRelationship tdsRelationship : tdsMetadata.getTdsRelationships()) {
            TdsMetadata.TdsColumn col1 = tdsRelationship.getCol1();
            TdsMetadata.TdsColumn col2 = tdsRelationship.getCol2();

            TdsMetadata.TdsTable tbl1 = tdsMetadata.getTdsTable(col1.getTdsTableId());
            TdsMetadata.TdsTable tbl2 = tdsMetadata.getTdsTable(col2.getTdsTableId());
        }
//
//        for (TdsMetadata.TdsRelationship tdsRelationship: tdsMetadata.getTdsRelationships()) {
//            log.info("RELATIONSHIP => {}", tdsRelationship);
//        }
    }

    public void writeToFile(String fileName) throws Exception {
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("UTF-8");

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8)) {
            XMLWriter xmlWriter = new XMLWriter(writer, format);
            xmlWriter.write(document);
            xmlWriter.close();
            log.info("XML file built successfully: {}", fileName);
        }
    }

    public static void main(String[] args) throws Exception {
//        demo011_1con_ntable();
//        demo011_1con_ntable_nrelationship();
//        demo012_ncon_ntable();
        demo012_ncon_ntable_nrel();
//        demo012_ncon_ntable()_nrelationship();
    }

    private static void demo011_1con_ntable() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TdsMetadata.TableauInfo tableauInfo = new TdsMetadata.TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsMetadata.TdsConnection conn1 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        tdsMetadata.addTdsConnection(conn1);

        TdsMetadata.TdsTable tb1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb2 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb3 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb4 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable tb5 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable tb6 = new TdsMetadata.TdsTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, tb1);
        tdsMetadata.addTdsTable(conn1, tb2);
        tdsMetadata.addTdsTable(conn1, tb3);
        tdsMetadata.addTdsTable(conn1, tb4);
        tdsMetadata.addTdsTable(conn1, tb5);
        tdsMetadata.addTdsTable(conn1, tb6);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds/011_1con_ntbl.tds");
        // tabcmd publish ./output/tds/011_1con_ntbl.tds --project='[ict_1] ts_tds_builder' --overwrite
    }

    private static void demo011_1con_ntable_nrelationship() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TdsMetadata.TableauInfo tableauInfo = new TdsMetadata.TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsMetadata.TdsConnection conn1 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        tdsMetadata.addTdsConnection(conn1);

        TdsMetadata.TdsTable tb1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb2 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb3 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable tb4 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable tb5 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable tb6 = new TdsMetadata.TdsTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, tb1);
        tdsMetadata.addTdsTable(conn1, tb2);
        tdsMetadata.addTdsTable(conn1, tb3);
        tdsMetadata.addTdsTable(conn1, tb4);
        tdsMetadata.addTdsTable(conn1, tb5);
        tdsMetadata.addTdsTable(conn1, tb6);

        TdsMetadata.TdsColumn col1 = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn col2 = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn col3 = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn col4 = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn col5 = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn col6 = new TdsMetadata.TdsColumn("code");
        tdsMetadata.addTdsColumn(tb1, col1);
        tdsMetadata.addTdsColumn(tb2, col2);
        tdsMetadata.addTdsColumn(tb3, col3);
        tdsMetadata.addTdsColumn(tb4, col4);
        tdsMetadata.addTdsColumn(tb5, col5);
        tdsMetadata.addTdsColumn(tb6, col6);

        TdsMetadata.TdsRelationship rel1 = new TdsMetadata.TdsRelationship(col1, col2);
        TdsMetadata.TdsRelationship rel2 = new TdsMetadata.TdsRelationship(col2, col3);
        TdsMetadata.TdsRelationship rel3 = new TdsMetadata.TdsRelationship(col1, col4);
        TdsMetadata.TdsRelationship rel4 = new TdsMetadata.TdsRelationship(col4, col5);
        TdsMetadata.TdsRelationship rel5 = new TdsMetadata.TdsRelationship(col1, col6);
//        tdsMetadata.addRelationship(rel1);
//        tdsMetadata.addRelationship(rel2);
//        tdsMetadata.addRelationship(rel3);
//        tdsMetadata.addRelationship(rel4);
//        tdsMetadata.addRelationship(rel5);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds/011_1con_ntbl_relationship.tds");
        // tabcmd publish ./output/tds/011_1con_ntbl_relationship.tds --project='[ict_1] ts_tds_builder' --overwrite
    }

    private static void demo012_ncon_ntable() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TdsMetadata.TableauInfo tableauInfo = new TdsMetadata.TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsMetadata.TdsConnection conn1 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        TdsMetadata.TdsConnection conn2 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        TdsMetadata.TdsConnection conn3 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        tdsMetadata.addTdsConnection(conn1);
        tdsMetadata.addTdsConnection(conn2);
        tdsMetadata.addTdsConnection(conn3);

        TdsMetadata.TdsTable conn1tbl1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn1tbl2 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn1tbl3 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn2tbl1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn2tbl2 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn2tbl3 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn3tbl1 = new TdsMetadata.TdsTable("ict.migration_current");
        TdsMetadata.TdsTable conn3tbl2 = new TdsMetadata.TdsTable("ict.migration_history");
        TdsMetadata.TdsTable conn3tbl3 = new TdsMetadata.TdsTable("ict.migration_cost");
        tdsMetadata.addTdsTable(conn1, conn1tbl1);
        tdsMetadata.addTdsTable(conn1, conn1tbl2);
        tdsMetadata.addTdsTable(conn1, conn1tbl3);
        tdsMetadata.addTdsTable(conn2, conn2tbl1);
        tdsMetadata.addTdsTable(conn2, conn2tbl2);
        tdsMetadata.addTdsTable(conn2, conn2tbl3);
        tdsMetadata.addTdsTable(conn3, conn3tbl1);
        tdsMetadata.addTdsTable(conn3, conn3tbl2);
        tdsMetadata.addTdsTable(conn3, conn3tbl3);

        TdsMetadata.TdsColumn conn1tbl1col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn1tbl2col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn1tbl3col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn2tbl1col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn2tbl2col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn2tbl3col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl1col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl2col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl3col = new TdsMetadata.TdsColumn("code");
        tdsMetadata.addTdsColumn(conn1tbl1, conn1tbl1col);
        tdsMetadata.addTdsColumn(conn1tbl2, conn1tbl2col);
        tdsMetadata.addTdsColumn(conn1tbl3, conn1tbl3col);
        tdsMetadata.addTdsColumn(conn2tbl1, conn2tbl1col);
        tdsMetadata.addTdsColumn(conn2tbl2, conn2tbl2col);
        tdsMetadata.addTdsColumn(conn2tbl3, conn2tbl3col);
        tdsMetadata.addTdsColumn(conn3tbl1, conn3tbl1col);
        tdsMetadata.addTdsColumn(conn3tbl2, conn3tbl2col);
        tdsMetadata.addTdsColumn(conn3tbl3, conn3tbl3col);

        TdsMetadata.TdsRelationship rel1 = new TdsMetadata.TdsRelationship(conn1tbl1col, conn1tbl2col);
        TdsMetadata.TdsRelationship rel2 = new TdsMetadata.TdsRelationship(conn2tbl1col, conn2tbl2col);
        TdsMetadata.TdsRelationship rel3 = new TdsMetadata.TdsRelationship(conn3tbl1col, conn3tbl2col);
        TdsMetadata.TdsRelationship rel4 = new TdsMetadata.TdsRelationship(conn3tbl3col, conn3tbl1col);
        TdsMetadata.TdsRelationship rel5 = new TdsMetadata.TdsRelationship(conn1tbl3col, conn3tbl1col);
        TdsMetadata.TdsRelationship rel6 = new TdsMetadata.TdsRelationship(conn1tbl2col, conn3tbl2col);
        TdsMetadata.TdsRelationship rel7 = new TdsMetadata.TdsRelationship(conn3tbl3col, conn1tbl1col);
//        tdsMetadata.addRelationship(rel1);
//        tdsMetadata.addRelationship(rel2);
//        tdsMetadata.addRelationship(rel3);
//        tdsMetadata.addRelationship(rel4);
//        tdsMetadata.addRelationship(rel5);
//        tdsMetadata.addRelationship(rel6);
//        tdsMetadata.addRelationship(rel7);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds/011_ncon_ntbl.tds");
        // tabcmd publish ./output/tds/011_ncon_ntbl.tds --project='[ict_1] ts_tds_builder' --overwrite
    }

    private static void demo012_ncon_ntable_nrel() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TdsMetadata.TableauInfo tableauInfo = new TdsMetadata.TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsMetadata.TdsConnection conn1 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        TdsMetadata.TdsConnection conn2 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        TdsMetadata.TdsConnection conn3 = new TdsMetadata.TdsConnection(
                "postgres",
                "3.35.93.207",
                "5432",
                "misolution_dev",
                "postgres",
                "Mlv_IT#25A"
        );
        tdsMetadata.addTdsConnection(conn1);
        tdsMetadata.addTdsConnection(conn2);
        tdsMetadata.addTdsConnection(conn3);

        TdsMetadata.TdsTable conn1tbl1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn1tbl2 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn1tbl3 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn2tbl1 = new TdsMetadata.TdsTable("public.test");
        TdsMetadata.TdsTable conn2tbl2 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn2tbl3 = new TdsMetadata.TdsTable("ict.migration_account");
        TdsMetadata.TdsTable conn3tbl1 = new TdsMetadata.TdsTable("ict.migration_current");
        TdsMetadata.TdsTable conn3tbl2 = new TdsMetadata.TdsTable("ict.migration_history");
        TdsMetadata.TdsTable conn3tbl3 = new TdsMetadata.TdsTable("ict.migration_cost");
        tdsMetadata.addTdsTable(conn1, conn1tbl1);
        tdsMetadata.addTdsTable(conn1, conn1tbl2);
        tdsMetadata.addTdsTable(conn1, conn1tbl3);
        tdsMetadata.addTdsTable(conn2, conn2tbl1);
        tdsMetadata.addTdsTable(conn2, conn2tbl2);
        tdsMetadata.addTdsTable(conn2, conn2tbl3);
        tdsMetadata.addTdsTable(conn3, conn3tbl1);
        tdsMetadata.addTdsTable(conn3, conn3tbl2);
        tdsMetadata.addTdsTable(conn3, conn3tbl3);

        TdsMetadata.TdsColumn conn1tbl1col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn1tbl2col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn1tbl3col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn2tbl1col = new TdsMetadata.TdsColumn("name");
        TdsMetadata.TdsColumn conn2tbl2col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn2tbl3col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl1col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl2col = new TdsMetadata.TdsColumn("code");
        TdsMetadata.TdsColumn conn3tbl3col = new TdsMetadata.TdsColumn("code");
        tdsMetadata.addTdsColumn(conn1tbl1, conn1tbl1col);
        tdsMetadata.addTdsColumn(conn1tbl2, conn1tbl2col);
        tdsMetadata.addTdsColumn(conn1tbl3, conn1tbl3col);
        tdsMetadata.addTdsColumn(conn2tbl1, conn2tbl1col);
        tdsMetadata.addTdsColumn(conn2tbl2, conn2tbl2col);
        tdsMetadata.addTdsColumn(conn2tbl3, conn2tbl3col);
        tdsMetadata.addTdsColumn(conn3tbl1, conn3tbl1col);
        tdsMetadata.addTdsColumn(conn3tbl2, conn3tbl2col);
        tdsMetadata.addTdsColumn(conn3tbl3, conn3tbl3col);

        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn1tbl1col, conn1tbl2col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn1tbl1col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn2tbl1col, conn2tbl2col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn2tbl1col, conn2tbl3col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn3tbl1col, conn3tbl2col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn3tbl1col, conn3tbl3col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn2tbl1col, conn3tbl2col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn2tbl2col, conn3tbl3col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn1tbl2col, conn3tbl1col));
        tdsMetadata.addRelationship(new TdsMetadata.TdsRelationship(conn1tbl3col, conn3tbl3col));

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds/012_ncon_ntbl_nrel.tds");
        // tabcmd publish ./output/tds/012_ncon_ntbl_nrel.tds --project='[ict_1] ts_tds_builder' --overwrite
    }
}
