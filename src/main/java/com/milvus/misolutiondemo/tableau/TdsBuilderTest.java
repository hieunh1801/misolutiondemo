package com.milvus.misolutiondemo.tableau;

import com.milvus.misolutiondemo.tableau.TdsMetadata.*;
import lombok.extern.slf4j.Slf4j;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class TdsBuilderTest {

    public static void main(String[] args) throws Exception {
//        demo011_1con_ntable();
//        demo011_1con_ntable_nrelationship();
//        demo012_ncon_ntable();
        demo012_ncon_ntable_nrel();
//        demo012_ncon_ntable()_nrelationship();
    }

    private static void demo011_1con_ntable() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsConnection conn1 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        tdsMetadata.addTdsConnection(conn1);

        TdsTable tb1 = TdsTable.createConnectionTable("public.test");
        TdsTable tb2 = TdsTable.createConnectionTable("public.test");
        TdsTable tb3 = TdsTable.createConnectionTable("public.test");
        TdsTable tb4 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable tb5 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable tb6 = TdsTable.createConnectionTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, tb1);
        tdsMetadata.addTdsTable(conn1, tb2);
        tdsMetadata.addTdsTable(conn1, tb3);
        tdsMetadata.addTdsTable(conn1, tb4);
        tdsMetadata.addTdsTable(conn1, tb5);
        tdsMetadata.addTdsTable(conn1, tb6);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds", "011_1con_ntbl");
        // tabcmd publish ./output/tds/011_1con_ntbl.tds --project='ts_tds_builder' --overwrite
    }

    private static void demo011_1con_ntable_nrelationship() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsConnection conn1 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        tdsMetadata.addTdsConnection(conn1);

        TdsTable tb1 = TdsTable.createConnectionTable("public.test");
        TdsTable tb2 = TdsTable.createConnectionTable("public.test");
        TdsTable tb3 = TdsTable.createConnectionTable("public.test");
        TdsTable tb4 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable tb5 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable tb6 = TdsTable.createConnectionTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, tb1);
        tdsMetadata.addTdsTable(conn1, tb2);
        tdsMetadata.addTdsTable(conn1, tb3);
        tdsMetadata.addTdsTable(conn1, tb4);
        tdsMetadata.addTdsTable(conn1, tb5);
        tdsMetadata.addTdsTable(conn1, tb6);

        TdsColumn col1 = new TdsColumn("name");
        TdsColumn col2 = new TdsColumn("name");
        TdsColumn col3 = new TdsColumn("name");
        TdsColumn col4 = new TdsColumn("code");
        TdsColumn col5 = new TdsColumn("code");
        TdsColumn col6 = new TdsColumn("code");
        tdsMetadata.addTdsColumn(tb1, col1);
        tdsMetadata.addTdsColumn(tb2, col2);
        tdsMetadata.addTdsColumn(tb3, col3);
        tdsMetadata.addTdsColumn(tb4, col4);
        tdsMetadata.addTdsColumn(tb5, col5);
        tdsMetadata.addTdsColumn(tb6, col6);

        TdsRelationship rel1 = new TdsRelationship(col1, col2);
        TdsRelationship rel2 = new TdsRelationship(col2, col3);
        TdsRelationship rel3 = new TdsRelationship(col1, col4);
        TdsRelationship rel4 = new TdsRelationship(col4, col5);
        TdsRelationship rel5 = new TdsRelationship(col1, col6);
//        tdsMetadata.addRelationship(rel1);
//        tdsMetadata.addRelationship(rel2);
//        tdsMetadata.addRelationship(rel3);
//        tdsMetadata.addRelationship(rel4);
//        tdsMetadata.addRelationship(rel5);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds", "011_1con_ntbl_relationship");
        // tabcmd publish ./output/tds/011_1con_ntbl_relationship.tds --project='ts_tds_builder' --overwrite
    }

    private static void demo012_ncon_ntable() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsConnection conn1 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        TdsConnection conn2 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        TdsConnection conn3 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        tdsMetadata.addTdsConnection(conn1);
        tdsMetadata.addTdsConnection(conn2);
        tdsMetadata.addTdsConnection(conn3);

        TdsTable conn1tbl1 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl2 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl3 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn2tbl1 = TdsTable.createConnectionTable("public.test");
        TdsTable conn2tbl2 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn2tbl3 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn3tbl1 = TdsTable.createConnectionTable("ict.migration_current");
        TdsTable conn3tbl2 = TdsTable.createConnectionTable("ict.migration_history");
        TdsTable conn3tbl3 = TdsTable.createConnectionTable("ict.migration_cost");
        tdsMetadata.addTdsTable(conn1, conn1tbl1);
        tdsMetadata.addTdsTable(conn1, conn1tbl2);
        tdsMetadata.addTdsTable(conn1, conn1tbl3);
        tdsMetadata.addTdsTable(conn2, conn2tbl1);
        tdsMetadata.addTdsTable(conn2, conn2tbl2);
        tdsMetadata.addTdsTable(conn2, conn2tbl3);
        tdsMetadata.addTdsTable(conn3, conn3tbl1);
        tdsMetadata.addTdsTable(conn3, conn3tbl2);
        tdsMetadata.addTdsTable(conn3, conn3tbl3);

        TdsColumn conn1tbl1col = new TdsColumn("name");
        TdsColumn conn1tbl2col = new TdsColumn("name");
        TdsColumn conn1tbl3col = new TdsColumn("code");
        TdsColumn conn2tbl1col = new TdsColumn("name");
        TdsColumn conn2tbl2col = new TdsColumn("code");
        TdsColumn conn2tbl3col = new TdsColumn("code");
        TdsColumn conn3tbl1col = new TdsColumn("code");
        TdsColumn conn3tbl2col = new TdsColumn("code");
        TdsColumn conn3tbl3col = new TdsColumn("code");
        tdsMetadata.addTdsColumn(conn1tbl1, conn1tbl1col);
        tdsMetadata.addTdsColumn(conn1tbl2, conn1tbl2col);
        tdsMetadata.addTdsColumn(conn1tbl3, conn1tbl3col);
        tdsMetadata.addTdsColumn(conn2tbl1, conn2tbl1col);
        tdsMetadata.addTdsColumn(conn2tbl2, conn2tbl2col);
        tdsMetadata.addTdsColumn(conn2tbl3, conn2tbl3col);
        tdsMetadata.addTdsColumn(conn3tbl1, conn3tbl1col);
        tdsMetadata.addTdsColumn(conn3tbl2, conn3tbl2col);
        tdsMetadata.addTdsColumn(conn3tbl3, conn3tbl3col);

        TdsRelationship rel1 = new TdsRelationship(conn1tbl1col, conn1tbl2col);
        TdsRelationship rel2 = new TdsRelationship(conn2tbl1col, conn2tbl2col);
        TdsRelationship rel3 = new TdsRelationship(conn3tbl1col, conn3tbl2col);
        TdsRelationship rel4 = new TdsRelationship(conn3tbl3col, conn3tbl1col);
        TdsRelationship rel5 = new TdsRelationship(conn1tbl3col, conn3tbl1col);
        TdsRelationship rel6 = new TdsRelationship(conn1tbl2col, conn3tbl2col);
        TdsRelationship rel7 = new TdsRelationship(conn3tbl3col, conn1tbl1col);
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
                .writeToFile("./output/tds", "011_ncon_ntbl");
        // tabcmd publish ./output/tds/011_ncon_ntbl.tds --project='ts_tds_builder' --overwrite
    }

    private static void demo012_ncon_ntable_nrel() throws Exception {
        TdsMetadata tdsMetadata = new TdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsConnection conn1 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        TdsConnection conn2 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        TdsConnection conn3 = TdsConnection.createConnection(
                "postgres",
                "username-password",
                "misolution_dev",
                "",
                "5432",
                "3.35.93.207",
                "misolution_dev",
                ""
        );
        tdsMetadata.addTdsConnection(conn1);
        tdsMetadata.addTdsConnection(conn2);
        tdsMetadata.addTdsConnection(conn3);

        TdsTable conn1tbl1 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl2 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl3 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn2tbl1 = TdsTable.createConnectionTable("public.test");
        TdsTable conn2tbl2 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn2tbl3 = TdsTable.createConnectionTable("ict.migration_account");
        TdsTable conn3tbl1 = TdsTable.createConnectionTable("ict.migration_current");
        TdsTable conn3tbl2 = TdsTable.createConnectionTable("ict.migration_history");
        TdsTable conn3tbl3 = TdsTable.createConnectionTable("ict.migration_cost");
        tdsMetadata.addTdsTable(conn1, conn1tbl1);
        tdsMetadata.addTdsTable(conn1, conn1tbl2);
        tdsMetadata.addTdsTable(conn1, conn1tbl3);
        tdsMetadata.addTdsTable(conn2, conn2tbl1);
        tdsMetadata.addTdsTable(conn2, conn2tbl2);
        tdsMetadata.addTdsTable(conn2, conn2tbl3);
        tdsMetadata.addTdsTable(conn3, conn3tbl1);
        tdsMetadata.addTdsTable(conn3, conn3tbl2);
        tdsMetadata.addTdsTable(conn3, conn3tbl3);

        TdsColumn conn1tbl1col = new TdsColumn("name");
        TdsColumn conn1tbl2col = new TdsColumn("name");
        TdsColumn conn1tbl3col = new TdsColumn("code");
        TdsColumn conn2tbl1col = new TdsColumn("name");
        TdsColumn conn2tbl2col = new TdsColumn("code");
        TdsColumn conn2tbl3col = new TdsColumn("code");
        TdsColumn conn3tbl1col = new TdsColumn("code");
        TdsColumn conn3tbl2col = new TdsColumn("code");
        TdsColumn conn3tbl3col = new TdsColumn("code");
        tdsMetadata.addTdsColumn(conn1tbl1, conn1tbl1col);
        tdsMetadata.addTdsColumn(conn1tbl2, conn1tbl2col);
        tdsMetadata.addTdsColumn(conn1tbl3, conn1tbl3col);
        tdsMetadata.addTdsColumn(conn2tbl1, conn2tbl1col);
        tdsMetadata.addTdsColumn(conn2tbl2, conn2tbl2col);
        tdsMetadata.addTdsColumn(conn2tbl3, conn2tbl3col);
        tdsMetadata.addTdsColumn(conn3tbl1, conn3tbl1col);
        tdsMetadata.addTdsColumn(conn3tbl2, conn3tbl2col);
        tdsMetadata.addTdsColumn(conn3tbl3, conn3tbl3col);

        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl1col, conn1tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl1col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(conn2tbl1col, conn2tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(conn2tbl1col, conn2tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(conn3tbl1col, conn3tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(conn3tbl1col, conn3tbl1col));
        tdsMetadata.addRelationship(new TdsRelationship(conn2tbl1col, conn3tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(conn2tbl2col, conn3tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl2col, conn3tbl1col));
        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl3col, conn3tbl3col));

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/tds", "@012_ncon_ntbl_nrel.tds");
        // tabcmd publish ./output/tds/@012_ncon_ntbl_nrel.tds --project='ts_tds_builder' --overwrite
    }
}
