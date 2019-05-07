package com.velotio.omnisci.utils.db;

import com.google.gson.*;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import com.omnisci.jdbc.OmniSciConnection;
import com.omnisci.jdbc.OmniSciDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorUtils {
//    private static final Logger LOG = LoggerFactory.getLogger(ProcessorUtils.class);
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.omnisci.jdbc.OmniSciDriver";
    static final String DB_URL = "jdbc:omnisci:localhost:6274:mapd";

    //  Database credentials
    static final String USER = "mapd";
    static final String PASS = "HyperInteractive";

    public static JsonArray ReadOmniSciTableJDBC(String tableName, String selArgs, String whereArgs, int limit, int offset) {
        Connection conn = null;
        Statement stmt = null;
        JsonArray jsonArray = new JsonArray();

        try {

            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // STEP 3: Execute a query
            stmt = conn.createStatement();

            String sql = "SELECT "+selArgs+" from "+tableName
                    +" "+ whereArgs;
            if(limit>0){
                sql = sql + " limit "+limit;
            }
            if (offset>0){
                sql = sql +" offset "+offset;
            }

            ResultSet rs = stmt.executeQuery(sql);

            // STEP 4: Extract data from result set
            ResultSetMetaData rsmd = rs.getMetaData();

            while(rs.next()) {
                int numColumns = rsmd.getColumnCount();

                JsonObject obj = new JsonObject();
                for (int i=1; i<=numColumns; i++) {
                    String column_name = rsmd.getColumnName(i);
                    obj.add(column_name, new Gson().toJsonTree(rs.getObject(column_name)));
                }
                jsonArray.add(obj);
            }

            // STEP 5: Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // finally block used to close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally try
        } // end try
        return jsonArray;
    } // end main

    public static String InsertIntoOmnisciTableJDBC(int insertBatchSize, String tableName, String isCreateTable, String tableSchema, JsonArray jsonArr, String userName, String password, String dbURL){
        Connection conn = null;
        PreparedStatement pStmt = null;
        try{
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // STEP 3: Execute a create table query
            if(isCreateTable.equalsIgnoreCase("TRUE") && tableSchema!=null && !tableSchema.equals("")) {
                Statement stmt = null;
                stmt = conn.createStatement();
                String createTableStmt = "CREATE table IF NOT EXISTS " + tableName + "(" + tableSchema + ")";
                stmt.executeUpdate(createTableStmt);
                stmt.close();
//                LOG.info("New Table "+tableName+" created successfully!");
            }

            //STEP 4: Create a Prepared Statement
            String sqlStatement = "SELECT * from "+tableName+" limit 1";
            Statement stmt = conn.createStatement();
            ResultSet rtst = stmt.executeQuery(sqlStatement);
            ResultSetMetaData rsmd = rtst.getMetaData();
            int dbColCount = rsmd.getColumnCount();

            String sqlInsertIntoOmnisciDB = "INSERT INTO "+tableName+" values(";
            for(int i=0;i<dbColCount;i++){
                if(i>0)
                    sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+", ?";
                else
                    sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+"?";
            }
            sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+")";
            pStmt = conn.prepareStatement(sqlInsertIntoOmnisciDB);
            int insertCount = 0;
            for(int i=0;i<jsonArr.size();i++) {
                JsonObject jsonInputObj = jsonArr.get(i).getAsJsonObject();
                Set<Map.Entry<String, JsonElement>> entries = jsonInputObj.entrySet();
                if(entries.size()!=dbColCount){
                    throw new Exception("Input Data does not match Table Schema");
                }
                for(int j=1;j<=dbColCount;j++) {
                    for (Map.Entry<String, JsonElement> entry : entries) {
                        if(entry.getKey().equals(rsmd.getColumnName(j))) {
                            switch (rsmd.getColumnTypeName(j)) {
                                case "SMALLINT":
                                    pStmt.setInt(j, entry.getValue().getAsInt());
                                    break;
                                case "BIGINT":
                                    pStmt.setInt(j, entry.getValue().getAsInt());
                                    break;
                                case "TIMESTAMP":
                                    pStmt.setTimestamp(j, Timestamp.valueOf(entry.getValue().getAsString()));
                                    break;
                                case "DATE":
                                    pStmt.setDate(j, Date.valueOf(entry.getValue().getAsString()));
                                    break;
                                case "FLOAT":
                                    pStmt.setFloat(j, entry.getValue().getAsFloat());
                                    break;
                                case "LONG":
                                    pStmt.setLong(j, entry.getValue().getAsLong());
                                    break;
                                case "STRING":
                                    pStmt.setString(j, entry.getValue().getAsString());
                                    break;
                                case "VARCHAR":
                                    pStmt.setString(j, entry.getValue().getAsString());
                                    break;
                                case "STR":
                                    pStmt.setString(j, entry.getValue().getAsString());
                                    break;
                                default:
                                    throw new Exception("Received Unhandled DataType in Input - " + rsmd.getColumnTypeName(j));
                            }
                        }
                    }
                }
                pStmt.addBatch();
                insertCount++;
                if(insertCount%insertBatchSize==0){
                    pStmt.executeBatch();
                    pStmt.clearBatch();
                    insertCount=0;
                }
            }
            if(insertCount>0){
                pStmt.executeBatch();
                pStmt.clearBatch();
            }
            pStmt.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
            return se.getMessage();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
            return e.getMessage();
        } finally {
            // finally block used to close resources
            try {
                if (pStmt != null) {
                    pStmt.close();
                }
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
                return se.getMessage();
            } // end finally try
        } // end try
        return "Success";
    }

    public static String InsertIntoOmnisciTableJDBC(int insertBatchSize, String tableName, String isCreateTable, String tableSchema, List<String> inputDataArr, String fileDelimiter, String userName, String password, String dbURL, String inputDataType){
        Connection conn = null;
        PreparedStatement pStmt = null;
        try{
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            conn = DriverManager.getConnection(dbURL, userName, password);

            // STEP 3: Execute a create table query
            if(isCreateTable.equalsIgnoreCase("TRUE") && tableSchema!=null && !tableSchema.equals("")) {
                Statement stmt = null;
                stmt = conn.createStatement();
                String createTableStmt = "CREATE table IF NOT EXISTS " + tableName + "(" + tableSchema + ")";
                stmt.executeUpdate(createTableStmt);
                stmt.close();
//                LOG.info("New Table "+tableName+" created successfully!");
            }

            //STEP 4: Create a Prepared Statement
            String sqlStatement = "SELECT * from "+tableName+" limit 1";
            Statement stmt = conn.createStatement();
            ResultSet rtst = stmt.executeQuery(sqlStatement);
            ResultSetMetaData rsmd = rtst.getMetaData();
            int dbColCount = rsmd.getColumnCount();

            String sqlInsertIntoOmnisciDB = "INSERT INTO "+tableName+" values(";
            for(int i=0;i<dbColCount;i++){
                if(i>0)
                    sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+", ?";
                else
                    sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+"?";
            }
            sqlInsertIntoOmnisciDB = sqlInsertIntoOmnisciDB+")";
            pStmt = conn.prepareStatement(sqlInsertIntoOmnisciDB);

            //STEP 5: Add data to the prepared Statement
            if(inputDataType.equals("Delimited")){
                for(int i=0;i<inputDataArr.size();i++){
                    String[] inputDataRow = inputDataArr.get(i).split(fileDelimiter);
                    if(dbColCount != inputDataRow.length){
                        throw new Exception("Input Data does not match Table Schema");
                    }
                    for(int j=1;j<=dbColCount;j++){
                        switch(rsmd.getColumnTypeName(j)){
                            case "SMALLINT":
                                pStmt.setInt(j,Integer.parseInt(inputDataRow[j-1].trim()));
                                break;
                            case "BIGINT":
                                pStmt.setInt(j,Integer.parseInt(inputDataRow[j-1].trim()));
                                break;
                            case "TIMESTAMP":
                                pStmt.setTimestamp(j,Timestamp.valueOf(inputDataRow[j-1].trim()));
                                break;
                            case "DATE":
                                pStmt.setDate(j,Date.valueOf(inputDataRow[j-1].trim()));
                                break;
                            case "FLOAT":
                                pStmt.setFloat(j,Float.parseFloat(inputDataRow[j-1].trim()));
                                break;
                            case "LONG":
                                pStmt.setLong(j,Long.parseLong(inputDataRow[j-1].trim()));
                                break;
                            case "STRING":
                                pStmt.setString(j,inputDataRow[j-1].trim());
                                break;
                            case "VARCHAR":
                                pStmt.setString(j,inputDataRow[j-1].trim());
                                break;
                            case "STR":
                                pStmt.setString(j,inputDataRow[j-1].trim());
                                break;
                            default:
                                throw new Exception("Received Unhandled DataType in Input - "+rsmd.getColumnTypeName(j));

                        }
                    }
                    pStmt.addBatch();
                    if(i%insertBatchSize==0){
                        pStmt.executeBatch();
                        pStmt.clearBatch();
                    }
                }
            }
            else if(inputDataType.equals("JSON")){
                for(int i=0;i<inputDataArr.size();i++) {
                    JsonObject jsonInputObj = new JsonParser().parse(inputDataArr.get(i)).getAsJsonObject();
                    Set<Map.Entry<String, JsonElement>> entries = jsonInputObj.entrySet();
                    if(entries.size()!=dbColCount){
                        throw new Exception("Input Data does not match Table Schema");
                    }
                    for(int j=1;j<=dbColCount;j++) {
                        for (Map.Entry<String, JsonElement> entry : entries) {
                            if(entry.getKey().equals(rsmd.getColumnName(j))) {
                                switch (rsmd.getColumnTypeName(j)) {
                                    case "SMALLINT":
                                        pStmt.setInt(j, entry.getValue().getAsInt());
                                        break;
                                    case "BIGINT":
                                        pStmt.setInt(j, entry.getValue().getAsInt());
                                        break;
                                    case "TIMESTAMP":
                                        pStmt.setTimestamp(j, Timestamp.valueOf(entry.getValue().getAsString()));
                                        break;
                                    case "DATE":
                                        pStmt.setDate(j, Date.valueOf(entry.getValue().getAsString()));
                                        break;
                                    case "FLOAT":
                                        pStmt.setFloat(j, entry.getValue().getAsFloat());
                                        break;
                                    case "LONG":
                                        pStmt.setLong(j, entry.getValue().getAsLong());
                                        break;
                                    case "STRING":
                                        pStmt.setString(j, entry.getValue().getAsString());
                                        break;
                                    case "VARCHAR":
                                        pStmt.setString(j, entry.getValue().getAsString());
                                        break;
                                    case "STR":
                                        pStmt.setString(j, entry.getValue().getAsString());
                                        break;
                                    default:
                                        throw new Exception("Received Unhandled DataType in Input - " + rsmd.getColumnTypeName(j));

                                }
                            }
                        }
                    }
                    pStmt.addBatch();
                    if(i%insertBatchSize==0){
                        pStmt.executeBatch();
                        pStmt.clearBatch();
                    }
                }
            }
            else{
                throw new Exception("Unhandled Input Data Type received! Please provide data in either Delimited or JSON Object format");
            }

            //STEP 6: Execute the Prepared Statement
            if(inputDataArr.size()%insertBatchSize>0) {
                pStmt.executeBatch();
            }
            pStmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
//            LOG.error("SQL Exception occurred: ",se);
            return se.getMessage();
        } catch (Exception e) {
            // Handle errors for Class.forName and user thrown exceptions
            e.printStackTrace();
//            LOG.error(e.getMessage(),e);
            return e.getMessage();
        } finally {
            // finally block used to close resources
            try {
                if (pStmt != null) {
                    pStmt.close();
                }
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally try
        } // end try
        return "Success";
    }

}
