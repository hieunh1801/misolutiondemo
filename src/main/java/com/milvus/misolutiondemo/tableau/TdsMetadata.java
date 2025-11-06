package com.milvus.misolutiondemo.tableau;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.swing.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Data
@EqualsAndHashCode
public class TdsMetadata {
    private int objectCount = 0;

    private TableauInfo tableauInfo;

    private List<TdsConnection> tdsConnections = new ArrayList<>();
    private Map<String, TdsConnection> tdsConnectionMap = new HashMap<>();

    private List<TdsTable> tdsTables = new ArrayList<>();
    private Map<String, TdsTable> tdsTableMap = new HashMap<>();

    private List<TdsColumn> tdsColumns = new ArrayList<>();
    private Map<String, TdsColumn> tdsColumnMap = new HashMap<>();

    private List<TdsRelationship> tdsRelationships = new ArrayList<>();
    private Map<String, TdsRelationship> tdsRelationshipMap = new HashMap<>();

    private Map<String, Integer> tableNameMap = new HashMap<>();
    private Set<String> columnNameSet = new HashSet<>();

    public void addTdsConnection(TdsConnection tdsConnection) {
        this.tdsConnections.add(tdsConnection);
        this.tdsConnectionMap.put(tdsConnection.getId(), tdsConnection);
    }

    public void addTdsTable(TdsConnection tdsConnection, TdsTable tdsTable) {
        tdsTable.setTdsConnectionId(tdsConnection.getId());
        this.tdsTables.add(tdsTable);
        this.tdsTableMap.put(tdsTable.getId(), tdsTable);
    }

    public void addTdsColumn(TdsTable tdsTable, TdsColumn tdsColumn) {
        tdsColumn.setTdsConnectionId(tdsTable.getTdsConnectionId());
        tdsColumn.setTdsTableId(tdsTable.getId());

        this.tdsColumns.add(tdsColumn);
        this.tdsColumnMap.put(tdsColumn.getId(), tdsColumn);
    }

    public void addRelationship(TdsRelationship tdsRelationship) {
        TdsColumn col1 = tdsRelationship.getCol1();
        TdsColumn col2 = tdsRelationship.getCol2();
        if (col1.equals(col2)) {
            new Exception("Col1 and Col2 are the same [" + col1.getName() + "] and [" + col2.getName() + "]").printStackTrace();
            return;
        }
        this.tdsRelationships.add(tdsRelationship);
        this.tdsRelationshipMap.put(tdsRelationship.getId(), tdsRelationship);
    }

    public TdsConnection getTdsConnection(String id) {
        return tdsConnectionMap.get(id);
    }

    public TdsTable getTdsTable(String id) {
        return tdsTableMap.get(id);
    }

    public TdsTable getTdsTableByObjectId(String objectId) {
        return tdsTables.stream()
                .filter(tbl -> tbl.getObjectId().equals(objectId))
                .findFirst()
                .orElse(null);
    }

    public TdsColumn getTdsColumn(String id) {
        return tdsColumnMap.get(id);
    }

    public void compute() {
        this.computeTdsTables();
        this.computeTdsColumns();
        this.computeRelationships();
    }

    private void computeTdsTables() {
        // STEP 1: update name of table
        Map<String, Integer> nameCounts = new HashMap<>();
        for (TdsTable table : tdsTables) {
            String originalName = table.getName();
            int count = nameCounts.getOrDefault(originalName, 0);
            if (count > 0) {
                String newName = originalName + count;
                table.setName(newName);
            }
            nameCounts.put(originalName, count + 1);
        }
        tdsTables.sort(Comparator.comparing(TdsTable::getName));
        for (TdsTable tdsTable : tdsTables) {
            int desiredLength = 10;
            objectCount = objectCount + 1;
            String objectId = new StringBuilder()
                    .append("OBJECT")
                    .append(String.format("%0" + desiredLength + "d", objectCount))
//                    .append("__")
//                    .append(TableauUtil.generateRandom(10))
                    .toString();
            tdsTable.setObjectId(objectId);
            tdsTable.setOrder(objectCount);
        }
    }

    private static void dfsTraverse(
            String node,
            Map<String, List<String>> graph,
            List<String> sortedList
    ) {
        if (sortedList.contains(node)) {
            log.info("NODE={} => X (VISITED)", node);
            return;
        }

        sortedList.add(node);
        List<String> children = graph.getOrDefault(node, new ArrayList<>());
        log.info("NODE={} => GRAPH={}", node, children);

        for (String childNode : children) {
            dfsTraverse(childNode, graph, sortedList);
        }
    }

    private void computeRelationships() {
        // order relationship by column2
        tdsRelationships.sort((rel1, rel2) -> {
            TdsColumn rel1col2 = rel1.getCol2();
            TdsColumn rel2col2 = rel2.getCol2();

            TdsTable rel1tbl2 = getTdsTable(rel1col2.getTdsTableId());
            TdsTable rel2tbl2 = getTdsTable(rel2col2.getTdsTableId());

            return rel1tbl2.getOrder() - rel2tbl2.getOrder();
        });

        // re-order table follow relationship
        List<String> sortedTdsTableObjectId = new ArrayList<>();
        Map<String, List<String>> graph = new HashMap<>();

        for (TdsTable tdsTable : tdsTables) {
            String objectId = tdsTable.getObjectId();
            graph.put(objectId, new ArrayList<>());
        }

        for (TdsRelationship tdsRelationship : tdsRelationships) {
            TdsColumn col1 = tdsRelationship.getCol1();
            TdsColumn col2 = tdsRelationship.getCol2();

            TdsTable tbl1 = getTdsTable(col1.getTdsTableId());
            TdsTable tbl2 = getTdsTable(col2.getTdsTableId());

            String objectId1 = tbl1.getObjectId();
            String objectId2 = tbl2.getObjectId();
            graph.get(objectId1).add(objectId2);
        }

        Set<String> allChildren = graph.values().stream() // childNode
                .flatMap(List::stream)
                .collect(Collectors.toSet());
        List<String> rootNodes = new ArrayList<>(new ArrayList<>(graph.keySet())
                .stream()
                .filter(node -> !allChildren.contains(node))
                .toList()); // rootNode
        rootNodes.sort(String::compareTo);


        for (String root : rootNodes) {
            dfsTraverse(root, graph, sortedTdsTableObjectId);
            log.info("--------");
        }

        List<TdsTable> sortedTdsTables = new ArrayList<>();
        for (String objectId : sortedTdsTableObjectId) {
            TdsTable tdsTable = getTdsTableByObjectId(objectId);
            sortedTdsTables.add(tdsTable);
        }
        this.tdsTables = sortedTdsTables;
        log.info("SORTED TABLE: {}", sortedTdsTableObjectId);
    }

    private void computeTdsColumns() {
        // update name
        Set<String> columnNameSet = new HashSet<>();
        for (TdsColumn tdsColumn : tdsColumns) {
            TdsTable tdsTable = getTdsTable(tdsColumn.getTdsTableId());
            String name = tdsColumn.getName();
            if (columnNameSet.contains(name)) {
                name = String.format("%s (%s)", name, tdsTable.getName());
                tdsColumn.setName(name);
            }
            columnNameSet.add(name);
        }
    }

    @Getter
    @EqualsAndHashCode
    public static class TableauInfo {
        private final String id;
        private String host;
        private String site;
        private String siteId;
        private String patName;
        private String patValue;

        public TableauInfo(String host, String site, String patName, String patValue) {
            this.id = UUID.randomUUID().toString();
            this.host = host;
            this.site = site;
            this.patName = patName;
            this.patValue = patValue;
        }
    }

    public enum ConnectionType {
        POSTGRES("postgres");

        private final String name;

        ConnectionType(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        public static ConnectionType from(String type) {
            if (type.equals("postgres")) {
                return POSTGRES;
            }
            return null;
        }
    }

    @Data
    @Getter
    @EqualsAndHashCode
    public static class TdsConnection {
        private final String id;
        private ConnectionType connectionType;
        private String host;
        private String port;
        private String database;
        private String username;
        private String password;

        // computed attribute
        private String namedConnection; // connectionType.random(26)

        public TdsConnection(String connectionType, String host, String port, String database, String username, String password) {
            this.id = UUID.randomUUID().toString();

            this.connectionType = ConnectionType.from(connectionType);
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.database = database;

            this.namedConnection = connectionType + "." + TableauUtil.generateRandom(28);
        }
    }

    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class TdsTable {
        @EqualsAndHashCode.Include
        private final String id;

        private String originalTable; // public.test
        private String table; // [public].[test]
        private String originalName; // test
        private String tdsConnectionId;

        // compute value
        private String objectId; // object-id of table
        private String name; // compute name (if table is duplicate)
        private int order; // order of table in tdsTables

        public TdsTable(String table) {
            this.id = UUID.randomUUID().toString();

            this.originalTable = table;
            String[] tableParts = table.split("\\.");
            this.table = Arrays.stream(tableParts).map(t -> "[" + t + "]").collect(Collectors.joining("."));
            this.originalName = tableParts[tableParts.length - 1];
            this.name = this.originalName;
//            this.objectId = TableauUtil.generateRandom(32).toUpperCase();
        }


    }

    @Data
    @EqualsAndHashCode
    public static class TdsColumn {
        private final String id;
        private String originalName; // use to save original name
        private String name; // compute name (if duplicate column between multiple table)
        private String type; // not use now

        private String tdsConnectionId; // link to connection
        private String tdsTableId; // link to table

        public TdsColumn(String name) {
            this.originalName = name;
            this.name = name;
            this.id = UUID.randomUUID().toString();
        }

        public String getDisplayName() {
            String input = name;
            if (input == null || input.isEmpty()) {
                return input;
            }
            String processed = input.replace('_', ' ').replace('-', ' ');
            StringBuilder result = new StringBuilder(processed.length());
            boolean capitalizeNext = true;
            for (char c : processed.toCharArray()) {
                if (Character.isWhitespace(c) || c == '(' || c == ')') {
                    result.append(c);
                    capitalizeNext = true;
                } else if (capitalizeNext) {
                    result.append(Character.toUpperCase(c));
                    capitalizeNext = false;
                } else {
                    result.append(Character.toLowerCase(c));
                }
            }
            return result.toString();
        }
    }

    @Data
    @EqualsAndHashCode
    public static class TdsRelationship {
        private final String id;
        private TdsColumn col1; // use to build Tds
        private TdsColumn col2;  // use to build Tds
        private String operator; // use to build Tds

        private boolean visited = false; // use to sort TdsRelationship

        public TdsRelationship(TdsColumn col1, TdsColumn col2, String operator) {
            this.id = UUID.randomUUID().toString();
            this.col1 = col1;
            this.col2 = col2;
            this.operator = operator;
        }

        public TdsRelationship(TdsColumn col1, TdsColumn col2) {
            this(col1, col2, "=");
        }
    }
}
