package com.velotio.omnisci.utils.db;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.omnisci.jdbc.OmniSciConnection;
import com.omnisci.jdbc.OmniSciDriver;

public class ProcessorUtils {
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

    public static String InsertIntoOmnisciTableJDBC(String tableName, JsonArray jsonArr){
        Connection conn = null;
        Statement stmt = null;
        try{
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // STEP 3: Execute a query
            stmt = conn.createStatement();
            String createTableStmt = "CREATE table IF NOT EXISTS "+tableName+"(arr_timestamp TIMESTAMP(0), dep_timestamp TIMESTAMP(0), uniquecarrier TEXT ENCODING DICT(32))";

            stmt.executeUpdate(createTableStmt);
            String preparedSQL = "insert into "+tableName+" values(?, ?, ?)";

            PreparedStatement pStmt = conn.prepareStatement(preparedSQL);
            for(int i=0;i<jsonArr.size();i++){
                pStmt.setTimestamp(1, Timestamp.valueOf(jsonArr.get(i).getAsJsonObject().get("arr_timestamp").getAsString()));
                pStmt.setTimestamp(2, Timestamp.valueOf(jsonArr.get(i).getAsJsonObject().get("dep_timestamp").getAsString()));
                pStmt.setString(3, jsonArr.get(i).getAsJsonObject().get("uniquecarrier").getAsString());
                pStmt.addBatch();
            }
            pStmt.executeBatch();
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
                return se.getMessage();
            } // end finally try
        } // end try
        return "Success";
    }

    public static String InsertIntoOmnisciTableJDBC(String tableName, String isCreateTable, String tableSchema, List<String> inputDataArr, String fileDelimiter, String userName, String password, String dbURL){
        Connection conn = null;
        PreparedStatement pStmt = null;
        try{
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            conn = DriverManager.getConnection(dbURL, userName, password);

            // STEP 3: Execute a create table query
            if(isCreateTable.equalsIgnoreCase("TRUE")) {
                Statement stmt = null;
                stmt = conn.createStatement();
                String createTableStmt = "CREATE table IF NOT EXISTS " + tableName + "(" + tableSchema + ")";
                stmt.executeUpdate(createTableStmt);
                stmt.close();
            }

            //STEP 4: Create a Prepared Statement
            String preparedSQL = "insert into "+tableName+" values";
            String[] tableSchemaCols = tableSchema.split(", ");
            for (int c=0; c< tableSchemaCols.length;c++) {
                if(c>0)
                    preparedSQL = preparedSQL.concat(", ?");
                else
                    preparedSQL = preparedSQL.concat("(?");
            }
            preparedSQL = preparedSQL.concat(")");
            pStmt = conn.prepareStatement(preparedSQL);

            //STEP 5: Add data to the prepared Statement
            for(int i=0;i<inputDataArr.size();i++){
                String[] inputRow = inputDataArr.get(i).split(fileDelimiter);
                if(inputRow.length!=tableSchemaCols.length){
                    continue;
                }
                for(int j=0; j<tableSchemaCols.length;j++){
                    if(tableSchemaCols[i].toUpperCase().contains("SMALLINT")){
                        pStmt.setInt(i, Integer.parseInt(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("TINYINT")){
                        pStmt.setInt(i, Integer.parseInt(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("INT")){
                        pStmt.setInt(i, Integer.parseInt(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("BIGINT")){
                        pStmt.setLong(i, Long.parseLong(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("FLOAT")){
                        pStmt.setFloat(i, Float.parseFloat(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("DECIMAL")){
                        pStmt.setFloat(i, Float.parseFloat(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("DOUBLE")){
                        pStmt.setDouble(i, Double.parseDouble(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("STR")){
                        pStmt.setString(i, inputRow[i].trim());
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("TIMESTAMP")){
                        pStmt.setTimestamp(i, Timestamp.valueOf(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("DATE")){
                        pStmt.setDate(i, Date.valueOf(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("BOOL")){
                        pStmt.setBoolean(i, Boolean.valueOf(inputRow[i].trim()));
                    }
                    else {
                        pStmt.setString(i, inputRow[i].trim());
                    }
                    /*
                    else if(tableSchemaCols[i].toUpperCase().contains("INTERVAL_DAY_TIME")){
                        pStmt.setFloat(i, Float.parseFloat(inputRow[i].trim()));
                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("INTERVAL_YEAR_MONTH")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("POINT")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("LINESTRING")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("POLYGON")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("MULTIPOLYGON")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("GEOMETRY")){

                    }
                    else if(tableSchemaCols[i].toUpperCase().contains("GEOGRAPHY")){

                    }*/

                }
                pStmt.addBatch();
            }

            //STEP 6: Execute the Prepared Statement
            pStmt.executeBatch();
            pStmt.close();
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
            } // end finally try
        } // end try
        return "Success";
    }

}
