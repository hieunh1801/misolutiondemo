package com.milvus.misolutiondemo.tableau;

public class TdsBuilderApplication {
    private static final String OUTPUT_FOLDER = "./output/";

    public static void main(String[] args) throws Exception {
//        TdsBuilderApplication.demo011_1con_ntable();
        TdsBuilderApplication.demo011_1con_ntable_relationship();
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
        TdsMetadata.TdsTable tb6= new TdsMetadata.TdsTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, tb1);
        tdsMetadata.addTdsTable(conn1, tb2);
        tdsMetadata.addTdsTable(conn1, tb3);
        tdsMetadata.addTdsTable(conn1, tb4);
        tdsMetadata.addTdsTable(conn1, tb5);
        tdsMetadata.addTdsTable(conn1, tb6);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/011_1con_ntbl.tds");
        // tabcmd publish 011_1con_ntbl_mix.tds --project='[ict_1] ts_tds_builder' --overwrite
    }

    private static void demo011_1con_ntable_relationship() throws Exception {
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
        TdsMetadata.TdsTable tb6= new TdsMetadata.TdsTable("ict.migration_account");
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
        TdsMetadata.TdsRelationship rel4 = new TdsMetadata.TdsRelationship(col1, col5);
        TdsMetadata.TdsRelationship rel5 = new TdsMetadata.TdsRelationship(col4, col5);
        TdsMetadata.TdsRelationship rel6 = new TdsMetadata.TdsRelationship(col2, col4);
        TdsMetadata.TdsRelationship rel7 = new TdsMetadata.TdsRelationship(col1, col6);
        TdsMetadata.TdsRelationship rel8 = new TdsMetadata.TdsRelationship(col6, col2);
        tdsMetadata.addRelationship(rel1);
        tdsMetadata.addRelationship(rel2);
        tdsMetadata.addRelationship(rel3);
        tdsMetadata.addRelationship(rel4);
        tdsMetadata.addRelationship(rel5);
        tdsMetadata.addRelationship(rel6);
        tdsMetadata.addRelationship(rel7);
        tdsMetadata.addRelationship(rel8);

        TdsBuilder tdsBuilder = new TdsBuilder(tdsMetadata);
        tdsBuilder
                .build()
                .writeToFile("./output/011_1con_ntbl_relationship.tds");
        // tabcmd publish 011_1con_ntbl_relationship.tds --project='[ict_1] ts_tds_builder' --overwrite
    }
}
