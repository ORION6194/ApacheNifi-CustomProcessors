package com.velotio.omnisci.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.sql.*;

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
                    +" "+ whereArgs + " limit "+limit +" offset "+offset;

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

    public static void main(String[] args){
//        JsonArray jsonArray = ReadOmniSciTableJDBC("flights_2008_7M","origin_city AS \"Origin\", dest_city AS \"Destination\", AVG(airtime) AS \"Average Airtime\"","WHERE distance < 175 GROUP BY origin_city, dest_city",100,0 );
//        System.out.println(jsonArray.size());
        String jsonInput = "[{'arr_timestamp':'2017-04-23 06:30:0','dep_timestamp':'2017-04-23 07:45:00','uniquecarrier':'Southwest'},{'arr_timestamp':'2017-04-23 06:50:0','dep_timestamp':'2017-04-23 09:45:00','uniquecarrier':'American'},{'arr_timestamp':'2017-04-23 09:30:0','dep_timestamp':'2017-04-23 12:45:00','uniquecarrier':'United'}]";
        InsertIntoOmnisciTableJDBC("kickFlights",new JsonParser().parse(jsonInput).getAsJsonArray());
    }

    public static String InsertIntoOmnisciTableJDBC(String tableName, JsonArray jsonArr){
        Connection conn = null;
        Statement stmt = null;
        int k=0;
        try{
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);
            k++;
            // STEP 2: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            k++;
            // STEP 3: Execute a query
            stmt = conn.createStatement();
            String createTableStmt = "CREATE table IF NOT EXISTS "+tableName+"(arr_timestamp TIMESTAMP(0), dep_timestamp TIMESTAMP(0), uniquecarrier TEXT ENCODING DICT(32))";
            k++;
            stmt.executeUpdate(createTableStmt);
            String preparedSQL = "insert into "+tableName+" values(?, ?, ?)";
            k++;
            PreparedStatement pStmt = conn.prepareStatement(preparedSQL);
            for(int i=0;i<jsonArr.size();i++){
                k++;
                pStmt.setTimestamp(1, Timestamp.valueOf(jsonArr.get(i).getAsJsonObject().get("arr_timestamp").getAsString()));
                pStmt.setTimestamp(2, Timestamp.valueOf(jsonArr.get(i).getAsJsonObject().get("dep_timestamp").getAsString()));
                pStmt.setString(3, jsonArr.get(i).getAsJsonObject().get("uniquecarrier").getAsString());
                pStmt.addBatch();
            }
            pStmt.executeBatch();
            k++;
            pStmt.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
            return se.getMessage()+"--JDBC";
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
            return e.getMessage()+"--Class.forName--"+k;
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
                return se.getMessage()+"--inFinally";
            } // end finally try
        } // end try
        return "Success";
    }
}
