package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;



public class DBConn {

    private String table;
    private String[] columns;
    private List<String> conditions = new LinkedList<String>();
    private List<String> sets = new LinkedList<String>();
    private String orderBy = "";
    public String connStr;

    public DBConn(String driver, String host, String port, String database, String user, String password) {
        this.connStr = "jdbc:"+driver+"://"+host+":"+port+"/"+database+"?"
                        + "user="+user+"&"
                        + "password="+password;
    }

    public DBConn select(String ...columns) {
        this.columns = columns;
        return this;
    }

    public DBConn into(String ...columns) {
        this.columns = columns;
        return this;
    }

    public DBConn insertInto(String table) {
        this.table = table;
        return this;
    }

    public DBConn from(String table) {
        this.table = table;
        return this;
    }

    public DBConn table(String table) {
        this.table = table;
        return this;
    }

    public DBConn set(String key, Object value) {
        if (value.getClass().getName() == "java.lang.String") {
            this.sets.add(key+"='"+value+"'");
        } else {
            this.sets.add(key+"="+value);
        }
        return this;
    }

    public DBConn where(Object key, String operator, Object value) {
        if (value.getClass().getName() == "java.lang.String") {
            if (operator.toUpperCase() == "LIKE") {
                value = "%"+value+"%";
            }
            this.conditions.add(key+" "+operator+" '"+value+"'");
        } else {
            this.conditions.add(key+" "+operator+" "+value);
        }
        return this;
    }

    public DBConn where(Object key, String operator, Object ...values) {
        List<String> strValues = new LinkedList<>();
        for (Object value : values) {
            if (value.getClass().getName() == "java.lang.String") {
                strValues.add("'"+value+"'");
            } else {
                strValues.add(value.toString());
            }
        }
        if (operator.toUpperCase() == "IN") {
            this.conditions.add(key+" IN ("+String.join(", ", strValues)+")");
        }
        if (operator.toUpperCase() == "NOT IN") {
            this.conditions.add(key+" NOT IN ("+String.join(", ", strValues)+")");
        }
        return this;
    }

    public DBConn where(String key, Object value) {
        if (value.getClass().getName() == "java.lang.String") {
            this.conditions.add(key+"='"+value+"'");
        } else {
            this.conditions.add(key+"="+value);
        }
        return this;
    }

    public DBConn orderBy(String key) {
        this.orderBy = " ORDER BY "+key;
        return this;
    }

    public DBConn orderBy(String key, String direction) {
        if (direction.toUpperCase() == "ASC") {
            this.orderBy = " ORDER BY "+key;
        }
        if (direction.toUpperCase() == "DESC") {
            this.orderBy = " ORDER BY "+key+" DESC";
        }
        return this;
    }

    public DBConn orderByDesc(String key) {
        this.orderBy = " ORDER BY "+key+" DESC";
        return this;
    }

    public SQLException delete() {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            
            String where = " WHERE "+String.join(" AND ", this.conditions);
            String sql = "DELETE FROM "+this.table+where;
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            return null;
        } catch (SQLException e) {
            return e;
        }
    }

    public SQLException update() {

        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            
            String sets = String.join(", ", this.sets);

            String where = " WHERE "+String.join(" AND ", this.conditions);
            String sql = "UPDATE "+this.table+" SET "+sets+where;
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            return null;
        } catch (SQLException e) {
            return e;
        }
    }

    public SQLException values(Object ...values) {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            List<String> valueList = new LinkedList<String>();
            for (Object value : values) {
                if (value.getClass().getName() == "java.lang.String") {
                    valueList.add("'"+value+"'");
                } else {
                    valueList.add(value.toString());
                }
            }
            String vString = "("+String.join(", ", valueList)+")";
            String sql = "INSERT INTO "+this.table+" VALUES "+vString;
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            return null;

        } catch (SQLException e) {
            return e;
        }
    }

    public SQLException insert(Object ...values) {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            List<String> valueList = new LinkedList<String>();
            for (Object value : values) {
                if (value.getClass().getName() == "java.lang.String") {
                    valueList.add("'"+value+"'");
                } else {
                    valueList.add(value.toString());
                }
                
            }
            String vString = "("+String.join(", ", valueList)+")";
            String columns = "";
            if (this.columns.length > 0) {
                columns = " ("+String.join(", ", columns)+")";
            }
            String sql = "INSERT INTO "+this.table+columns+" VALUES "+vString;
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);

            return null;

        } catch (SQLException e) {
            return e;
        }
    }

    public ArrayList<HashMap<String, Object>> get() {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            ResultSet resultSet = null;
            String columns = "*";
            String where = "";
            if (this.columns.length > 0) {
                columns = String.join(", ", this.columns);
            }
            if (this.conditions.size() > 0) {                
                where = " WHERE "+String.join(" AND ", this.conditions);
            }

            String sql = "SELECT "+columns+" FROM "+this.table+where+this.orderBy;
            System.out.println(sql);

            ArrayList<HashMap<String, Object>> arrayList = new ArrayList<HashMap<String, Object>>(); 
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {     
                HashMap<String, Object> row = new HashMap<String, Object>();                 
                int i = 1;
                while(i <= numberOfColumns) {
                    row.put(metadata.getColumnName(i), resultSet.getString(i));
                    i++;
                }
                arrayList.add(row);
            }
            return arrayList;
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public HashMap<String, Object> first() {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            ResultSet resultSet = null;
            String columns = "*";
            String where = "";
            if (this.columns.length > 0) {
                columns = String.join(", ", this.columns);
            }
            if (this.conditions.size() > 0) {                
                where = " WHERE "+String.join(" AND ", this.conditions);
            }

            String sql = "SELECT "+columns+" FROM "+this.table+where+this.orderBy+" LIMIT 1";
            System.out.println(sql);
            
            ArrayList<HashMap<String, Object>> arrayList = new ArrayList<HashMap<String, Object>>(); 
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {     
                HashMap<String, Object> row = new HashMap<String, Object>();                 
                int i = 1;
                while(i <= numberOfColumns) {
                    row.put(metadata.getColumnName(i), resultSet.getString(i));
                    i++;
                }
                arrayList.add(row);
            }
            return arrayList.get(0);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<HashMap<String, Object>> limit(Integer amount) {
        try (Connection connection = DriverManager.getConnection(this.connStr);) {
            ResultSet resultSet = null;
            String columns = "*";
            String where = "";
            if (this.columns.length > 0) {
                columns = String.join(", ", this.columns);
            }
            if (this.conditions.size() > 0) {                
                where = " WHERE "+String.join(" AND ", this.conditions);
            }

            String sql = "SELECT "+columns+" FROM "+this.table+where+this.orderBy+" LIMIT "+amount;
            System.out.println(sql);
            
            ArrayList<HashMap<String, Object>> arrayList = new ArrayList<HashMap<String, Object>>(); 
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {     
                HashMap<String, Object> row = new HashMap<String, Object>();                 
                int i = 1;
                while(i <= numberOfColumns) {
                    row.put(metadata.getColumnName(i), resultSet.getString(i));
                    i++;
                }
                arrayList.add(row);
            }
            return arrayList;
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
