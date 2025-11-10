package com.milvus.misolutiondemo.tableau;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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

    public TdsConnection addTdsConnection(TdsConnection tdsConnection) {
        this.tdsConnections.add(tdsConnection);
        this.tdsConnectionMap.put(tdsConnection.getId(), tdsConnection);
        return tdsConnection;
    }

    public TdsTable addTdsTable(TdsConnection tdsConnection, TdsTable tdsTable) {
        tdsTable.setTdsConnectionId(tdsConnection.getId());
        this.tdsTables.add(tdsTable);
        this.tdsTableMap.put(tdsTable.getId(), tdsTable);
        return tdsTable;
    }

    public TdsColumn addTdsColumn(TdsTable tdsTable, TdsColumn tdsColumn) {
        tdsColumn.setTdsConnectionId(tdsTable.getTdsConnectionId());
        tdsColumn.setTdsTableId(tdsTable.getId());

        this.tdsColumns.add(tdsColumn);
        this.tdsColumnMap.put(tdsColumn.getId(), tdsColumn);
        return tdsColumn;
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
        this.computeTdsConnections();
        this.computeTdsTables();
        this.computeTdsColumns();
        this.computeRelationships();
    }

    private void computeTdsConnections() {
        // 1. sort by name
        tdsConnections.sort(Comparator.comparing(TdsConnection::getNamedConnectionCaption));

        // 2. add (1), ... to namedConnectionCaption
        Map<String, Integer> namedConnectionCaptionCount = new HashMap<>();
        for (TdsConnection tdsConnection : tdsConnections) {
            String originalName = tdsConnection.getNamedConnectionCaption();
            int count = namedConnectionCaptionCount.getOrDefault(originalName, 0);
            if (count > 0) {
                String newName = originalName + count;
                tdsConnection.setNamedConnectionCaption(newName);
            }
            namedConnectionCaptionCount.put(originalName, count + 1);
        }

        // 3. add namedConnectionName
        int desiredLength = 10;
        int connectionCount = 0;
        for (TdsConnection tdsConnection : tdsConnections) {
            connectionCount = connectionCount + 1;
            String name = new StringBuilder()
                    .append("CONNECTION_")
                    .append(String.format("%0" + desiredLength + "d", connectionCount))
                    .toString();
            tdsConnection.setNamedConnectionName(name);
        }
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
                    .append("OBJECT_")
                    .append(String.format("%0" + desiredLength + "d", objectCount))
//                    .append("__")
//                    .append(TableauUtil.generateRandom(10))
                    .toString();
            tdsTable.setObjectId(objectId);
            tdsTable.setOrder(objectCount);
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

//    public enum ConnectionType {
//        POSTGRES("postgres"),
//        HYPER("hyper");
//
//        private final String name;
//
//        ConnectionType(String name) {
//            this.name = name;
//        }
//
//        public String getName() {
//            return this.name;
//        }
//
//        public static ConnectionType from(String type) {
//            if (type.equals("postgres")) {
//                return POSTGRES;
//            }
//            return null;
//        }
//    }

    @Data
    @Getter
    @EqualsAndHashCode
    public static class TdsConnection {
        private final String id;
        private String type; // connection | data

        private String connectionClass;
        // type=data & connectionClass=textscan;
        private String connectionDirectory;
        private String connectionFilename;
        // type=connection & connectionClass=postgres
        private String connectionAuthentication;
        private String connectionDbName;
        private String connectionOneTimeSql;
        private String connectionPort;
        private String connectionServer;
        private String connectionUsername;
        private String connectionWorkgroupAuthMode;

        // computed attribute
        private String namedConnectionName; // connectionType.random(26)
        private String namedConnectionCaption;

        private TdsConnection() {
            this.id = UUID.randomUUID().toString();
        }

        public static TdsConnection createConnection(String connectionClass,
                                                     String connectionAuthentication,
                                                     String connectionDbName,
                                                     String connectionOneTimeSql,
                                                     String connectionPort,
                                                     String connectionServer,
                                                     String connectionUsername,
                                                     String connectionWorkgroupAuthMode) {
            TdsConnection tdsConnection = new TdsConnection();
            tdsConnection.setType("connection");
            tdsConnection.setConnectionClass(connectionClass);
            tdsConnection.setConnectionAuthentication(connectionAuthentication);
            tdsConnection.setConnectionDbName(connectionDbName);
            tdsConnection.setConnectionOneTimeSql(connectionOneTimeSql);
            tdsConnection.setConnectionPort(connectionPort);
            tdsConnection.setConnectionServer(connectionServer);
            tdsConnection.setConnectionUsername(connectionUsername);
            tdsConnection.setConnectionWorkgroupAuthMode(connectionWorkgroupAuthMode);

            tdsConnection.setNamedConnectionCaption(connectionServer);
            return tdsConnection;
        }

        public static TdsConnection createCsvConnection(String connectionDirectory, String connectionFilename) {
            TdsConnection tdsConnection = new TdsConnection();
            tdsConnection.setType("data");
            tdsConnection.setConnectionClass("textscan");
            tdsConnection.setConnectionDirectory(connectionDirectory);
            tdsConnection.setConnectionFilename(connectionFilename);

            tdsConnection.setNamedConnectionCaption(connectionFilename);
            return tdsConnection;
        }
    }

    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class TdsTable {
        @EqualsAndHashCode.Include
        private final String id;

        private String type; // data | connection
        private String originalTable; // public.test
        private String table; // [public].[test]
        private String originalName; // test
        private String tdsConnectionId;

        // compute value
        private String objectId; // object-id of table
        private String name; // compute name (if table is duplicate)
        private int order; // order of table in tdsTables

        private TdsTable() {
            this.id = UUID.randomUUID().toString();
        }

        public TdsTable(String table) {
            this.id = UUID.randomUUID().toString();
            this.originalTable = table;
            String[] tableParts = table.split("\\.");
            this.table = Arrays.stream(tableParts).map(t -> "[" + t + "]").collect(Collectors.joining("."));
            this.originalName = tableParts[tableParts.length - 1];
            this.name = this.originalName;
            this.type = "connection";
//            this.objectId = TableauUtil.generateRandom(32).toUpperCase();
        }

        /**
         * @param table: this is table path
         *               Ex. public.tableName
         * @return TdsTable
         */
        public static TdsTable createConnectionTable(String table) {
            TdsTable tdsTable = new TdsTable();
            tdsTable.originalTable = table;
            String[] tableParts = table.split("\\.");
            tdsTable.table = Arrays.stream(tableParts).map(t -> "[" + t + "]").collect(Collectors.joining("."));
            tdsTable.originalName = tableParts[tableParts.length - 1];
            tdsTable.name = tdsTable.originalName;
            tdsTable.type = "connection";
            return tdsTable;
        }

        /**
         * @param name: this is table name
         * @return tdsTale
         */
        public static TdsTable createCsvTable(String name, String fileName) {
            TdsTable tdsTable = new TdsTable();
            tdsTable.name = name;
            tdsTable.table = String.format("[%s#csv]", fileName);
            tdsTable.type = "data";
            return tdsTable;
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
