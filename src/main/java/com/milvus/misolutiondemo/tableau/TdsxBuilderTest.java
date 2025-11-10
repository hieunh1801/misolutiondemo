package com.milvus.misolutiondemo.tableau;

import com.milvus.misolutiondemo.tableau.TdsMetadata.TableauInfo;
import com.milvus.misolutiondemo.tableau.TdsMetadata.TdsColumn;
import com.milvus.misolutiondemo.tableau.TdsMetadata.TdsConnection;
import com.milvus.misolutiondemo.tableau.TdsMetadata.TdsTable;
import com.milvus.misolutiondemo.tableau.TdsMetadata.TdsRelationship;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class TdsxBuilderTest {

    public static void main(String[] args) throws Exception {
//        demo_mix001_1data();
//        demo_mix002_ndata();
//        demo_mix003_ndata_nrel();
        demo_mix004_ndata_ncon();
    }

    private static void demo_mix001_1data() throws Exception {
        String fileName = "mix001_1data";
        String outputFolder = "./output/tdsx";
        TdsxBuilder tdsxBuilder = new TdsxBuilder(fileName, outputFolder);
        TdsMetadata tdsMetadata = tdsxBuilder.getTdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsTable tbl1 = tdsxBuilder.createCsv("users", CsvBuilder.createMockData("name, email, password", 10));
        TdsColumn tbl1col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl1, tbl1col);
        tdsxBuilder.build().zip();
    }

    private static void demo_mix002_ndata() throws Exception {
        String fileName = "mix002_ndata";
        String outputFolder = "./output/tdsx";
        TdsxBuilder tdsxBuilder = new TdsxBuilder(fileName, outputFolder);
        TdsMetadata tdsMetadata = tdsxBuilder.getTdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsTable tbl1 = tdsxBuilder.createCsv("users", CsvBuilder.createMockData("name, email, created_at", 10));
        TdsColumn tbl1col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl1, tbl1col);

        TdsTable tbl2 = tdsxBuilder.createCsv("orders", CsvBuilder.createMockData("user_id, total_amount, status, created_at", 10));
        TdsColumn tbl2col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl2, tbl2col);

        TdsTable tbl3 = tdsxBuilder.createCsv("order_items", CsvBuilder.createMockData("order_id, product_name, quantity, price", 10));
        TdsColumn tbl3col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl3, tbl3col);

        tdsxBuilder.build().zip();
//        tdsxBuilder.build();
    }


    private static void demo_mix003_ndata_nrel() throws Exception {
        String fileName = "mix003_ndata_nrel";
        String outputFolder = "./output/tdsx";
        TdsxBuilder tdsxBuilder = new TdsxBuilder(fileName, outputFolder);
        TdsMetadata tdsMetadata = tdsxBuilder.getTdsMetadata();
        TableauInfo tableauInfo = new TableauInfo(
                "https://t.tableau-report.com",
                "ict_1",
                "",
                ""
        );
        tdsMetadata.setTableauInfo(tableauInfo);
        TdsTable tbl1 = tdsxBuilder.createCsv("users", CsvBuilder.createMockData("name, email, created_at", 10));
        TdsColumn tbl1col = new TdsColumn("id");
        TdsColumn tbl1col2 = new TdsColumn("name");
        TdsColumn tbl1col3 = new TdsColumn("email");
        tdsMetadata.addTdsColumn(tbl1, tbl1col);
        tdsMetadata.addTdsColumn(tbl1, tbl1col2);

        TdsTable tbl2 = tdsxBuilder.createCsv("orders", CsvBuilder.createMockData("user_id, total_amount, status, created_at", 10));
        TdsColumn tbl2col = new TdsColumn("id");
        TdsColumn tbl2col2 = new TdsColumn("total_amount");
        tdsMetadata.addTdsColumn(tbl2, tbl2col);
        tdsMetadata.addTdsColumn(tbl2, tbl2col2);

        TdsTable tbl3 = tdsxBuilder.createCsv("order_items", CsvBuilder.createMockData("order_id, product_name, quantity, price", 10));
        TdsColumn tbl3col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl3, tbl3col);

        TdsTable tbl4 = tdsxBuilder.createCsv("order_items", CsvBuilder.createMockData("order_id, product_name, quantity, price", 10));
        TdsColumn tbl4col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl4, tbl4col);

        TdsTable tbl5 = tdsxBuilder.createCsv("order_items", CsvBuilder.createMockData("order_id, product_name, quantity, price", 10));
        TdsColumn tbl5col = new TdsColumn("id");
        tdsMetadata.addTdsColumn(tbl5, tbl5col);

        tdsMetadata.addRelationship(new TdsRelationship(tbl1col, tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(tbl1col, tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(tbl2col, tbl4col));
        tdsMetadata.addRelationship(new TdsRelationship(tbl1col, tbl4col));
        tdsMetadata.addRelationship(new TdsRelationship(tbl1col, tbl5col));
        tdsMetadata.addRelationship(new TdsRelationship(tbl5col, tbl3col));

        tdsxBuilder.build().zip();
//        tdsxBuilder.build();
    }


    private static void demo_mix004_ndata_ncon() throws Exception {
        String fileName = "mix005_ndata_ncon";
        String outputFolder = "./output/tdsx";
        TdsxBuilder tdsxBuilder = new TdsxBuilder(fileName, outputFolder);
        TdsMetadata tdsMetadata = tdsxBuilder.getTdsMetadata();
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
        TdsTable conn1tbl1 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl2 = TdsTable.createConnectionTable("public.test");
        TdsTable conn1tbl3 = TdsTable.createConnectionTable("ict.migration_account");
        tdsMetadata.addTdsTable(conn1, conn1tbl1);
        tdsMetadata.addTdsTable(conn1, conn1tbl2);
        tdsMetadata.addTdsTable(conn1, conn1tbl3);
        TdsColumn conn1tbl1col = new TdsColumn("name");
        TdsColumn conn1tbl2col = new TdsColumn("name");
        TdsColumn conn1tbl3col = new TdsColumn("code");
        tdsMetadata.addTdsColumn(conn1tbl1, conn1tbl1col);
        tdsMetadata.addTdsColumn(conn1tbl2, conn1tbl2col);
        tdsMetadata.addTdsColumn(conn1tbl3, conn1tbl3col);

        TdsTable data1tbl1 = tdsxBuilder.createCsv("tbl_users", CsvBuilder.createMockData("user_name, email, password", 10));
        TdsColumn data1tbl1col = new TdsColumn("user_name");
        tdsMetadata.addTdsColumn(data1tbl1, data1tbl1col);
        TdsTable data2tbl1 = tdsxBuilder.createCsv("tbl_products", CsvBuilder.createMockData("product_name, email, password", 10));
        TdsColumn data2tbl1col = new TdsColumn("product_name");
        tdsMetadata.addTdsColumn(data2tbl1, data2tbl1col);
        TdsTable data3tbl1 = tdsxBuilder.createCsv("tbl_product_details", CsvBuilder.createMockData("name, email, password", 10));
        TdsColumn data3tbl1col = new TdsColumn("name");
        tdsMetadata.addTdsColumn(data3tbl1, data3tbl1col);
        TdsTable data4tbl1 = tdsxBuilder.createCsv("tbl_insight_account", CsvBuilder.createMockData("name, email, password", 10));
        TdsColumn data4tbl1col = new TdsColumn("name");
        tdsMetadata.addTdsColumn(data4tbl1, data4tbl1col);
        TdsTable data5tbl1 = tdsxBuilder.createCsv("tbl_insight_history", CsvBuilder.createMockData("name, email, password", 10));
        TdsColumn data5tbl1col = new TdsColumn("name");
        tdsMetadata.addTdsColumn(data5tbl1, data5tbl1col);

        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl1col, conn1tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl1col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(conn1tbl2col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(data1tbl1col, conn1tbl2col));
        tdsMetadata.addRelationship(new TdsRelationship(data2tbl1col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(data3tbl1col, conn1tbl3col));
        tdsMetadata.addRelationship(new TdsRelationship(data4tbl1col, data5tbl1col));
        tdsMetadata.addRelationship(new TdsRelationship(data3tbl1col, data5tbl1col));
        tdsMetadata.addRelationship(new TdsRelationship(data2tbl1col, conn1tbl2col));


        tdsxBuilder.build().zip();
    }


}
