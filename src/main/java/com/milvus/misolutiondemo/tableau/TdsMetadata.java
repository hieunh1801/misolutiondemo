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
        this.tdsRelationships.add(tdsRelationship);
        this.tdsRelationshipMap.put(tdsRelationship.getId(), tdsRelationship);
    }

    public TdsConnection getTdsConnection(String id) {
        return tdsConnectionMap.get(id);
    }

    public TdsTable getTdsTable(String id) {
        return tdsTableMap.get(id);
    }

    public TdsColumn getTdsColumn(String id) {
        return tdsColumnMap.get(id);
    }

    public void compute() {
        // compute tdsTables
        this.computeTdsTables();
        this.computeTdsColumns();
    }

    private void computeTdsTables() {
        Map<String, List<TdsTable>> grouped = new HashMap<>();
        // sort table by name
        tdsTables.sort(Comparator.comparing(TdsTable::getName));
        for (TdsTable t : tdsTables) {
            grouped.computeIfAbsent(t.getName(), k -> new ArrayList<>()).add(t);
        }
        List<TdsTable> result = new ArrayList<>();
        for (Map.Entry<String, List<TdsTable>> entry : grouped.entrySet()) {
            List<TdsTable> group = entry.getValue();
            // revert table name
            if (group.size() > 1) {
                int counter = group.size();
                for (TdsTable t : group) {
                    if (counter == group.size()) {
                        // keep the first name
                    } else {
                        t.setName(t.getName() + counter);
                    }
                    counter--;
                }
            }
            result.addAll(group);
        }

        for (TdsTable tdsTable : result) {
            int desiredLength = 10;
            objectCount = objectCount + 1;
            String objectId = new StringBuilder()
                    .append("OBJECT")
                    .append(String.format("%0" + desiredLength + "d", objectCount))
                    .append("|")
                    .append(TableauUtil.generateRandom(10))
                    .toString();
            tdsTable.setObjectId(objectId);
        }

        this.tdsTables = result;
        for (TdsTable tdsTable : tdsTables) {
            log.info("TABLE [{}]", tdsTable.getName());
        }
    }

    private void computeTdsColumns() {
        Set<String> columnNameSet = new HashSet<>();
        // update name
        for (TdsColumn tdsColumn : tdsColumns) {
            TdsTable tdsTable = getTdsTable(tdsColumn.getTdsTableId());
            String name = tdsColumn.getName();
            if (columnNameSet.contains(name)) {
                name = String.format("%s (%s)", name, tdsTable.getName());
                tdsColumn.setName(name);
            }
            columnNameSet.add(name);
        }

        for (TdsColumn tdsColumn : tdsColumns) {
            TdsTable tdsTable = getTdsTable(tdsColumn.getTdsTableId());
            log.info("COLUMN [{}] TABLE [{}]", tdsColumn.getName(), tdsTable.getName());
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
    @EqualsAndHashCode
    public static class TdsTable {
        private final String id;
        private String originalTable; // public.test
        private String table; // [public].[test]
        private String originalName; // test
        private String tdsConnectionId;

        // compute value
        private String objectId; // object-id of table
        private String name; // test

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
        private String originalName;
        private String name;
        private String type;

        private String tdsConnectionId;
        private String tdsTableId;

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

    @Getter
    @EqualsAndHashCode
    public static class TdsRelationship {
        private final String id;
        private TdsColumn col1;
        private TdsColumn col2;
        private String operator;

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
