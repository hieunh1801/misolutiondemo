```bash
# delete and publish datasource
export datasource=011_multiple_test_table_builder_3tbl 
export datasource=011_multiple_test_table_builder_4tbl
tabcmd delete -d $datasource --project="ts_tds_builder" 
tabcmd publish $datasource.tds --project='ts_tds_builder' --overwrite
```

```bash
# publish tds_template
cd ./template/tds_template/
tabcmd publish 011_1con_3tbl.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_1con_3tbl_join.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_1con_4tbl.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_1con_4tbl_join.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_multiple_test_table_5tbl.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_multiple_test_table_6tbl.tds --project='ts_tds_builder' --overwrite
tabcmd publish 011_1con_1tbl.tds --project='ts_tds_builder' --overwrite

tabcmd publish ./template/tds_template/@011_ncon_ntbl.tds --project='ts_tds_builder' --overwrite
tabcmd publish ./template/tds_template/012_ncon_ntbl_nrel.tds --project='ts_tds_builder' --overwrite
tabcmd publish ./output/tds/@011_ncon_ntbl.tds --project='ts_tds_builder' --overwrite

tabcmd publish ./output/hyper/order.hyper --project='ts_tds_builder' --overwrite
```