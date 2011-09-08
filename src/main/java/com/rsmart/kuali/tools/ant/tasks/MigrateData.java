/*
 * Copyright 2005-2007 The Kuali Foundation
 *
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rsmart.kuali.tools.ant.tasks;

import java.io.PrintStream;
import java.io.Reader;

import java.lang.reflect.Field;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Main;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.pool.OracleDataSource;

import static org.apache.tools.ant.Project.MSG_DEBUG;

/**
 *
 * @author Leo Przybylski (przybyls@arizona.edu)
 */
public class MigrateData extends Task {
    
    private static final String RECORD_COUNT_QUERY = "select count(*) as COUNT from %s";
    private static final String SELECT_ALL_QUERY   = "select * from %s";
    private static final String INSERT_STATEMENT   = "insert into %s (%s) values (%s)";
    private static final String DATE_CONVERSION    = "TO_DATE('%s', 'YYYYMMDDHH24MISS')";
    private static final String COUNT_FIELD        = "COUNT";
    private static final String LIQUIBASE_TABLE    = "DATABASECHANGELOG";
    private static final int[]  QUOTED_TYPES       = 
        new int[] {Types.CHAR, Types.VARCHAR, Types.TIME, Types.LONGVARCHAR, Types.DATE, Types.TIMESTAMP};

    private static final String HSQLDB_PUBLIC      = "PUBLIC";
    private static final int    MAX_THREADS        = 2;

    
    private String source;
    private String target;
    private int threadCount;

    public MigrateData() { 
        int threadCount = 1;
    }

    
    public void setSource(String refid) {
        this.source = refid;
    }
    
    public String getSource() {
        return this.source;
    }

    public void setTarget(String refid) {
        this.target = refid;
    }

    public String getTarget() {
        return this.target;
    }
    
    public void execute() {
        final RdbmsConfig source = (RdbmsConfig) getProject().getReference(getSource());
        final RdbmsConfig target = (RdbmsConfig) getProject().getReference(getTarget());

        log("Migrating data from " + source.getUrl() + " to " + target.getUrl());

        final Incrementor recordCountIncrementor = new Incrementor();
        final Map<String, Integer> tableData = getTableData(source, target, recordCountIncrementor);

        log("Copying " + tableData.size() + " tables");

        float recordVisitor = 0;
        final ProgressObserver progressObserver = new ProgressObserver(recordCountIncrementor.getValue(),
                                                                       48f, 48f/100,
                                                                       "\r|%s%%| %s (%d/%d) records");
        final ProgressObservable observable = new ProgressObservable();
        observable.addObserver(progressObserver);

        for (final String tableName : tableData.keySet()) {
            debug("Migrating table " + tableName + " with " + tableData.get(tableName) + " records");
            if (!(tableName.toUpperCase().startsWith(LIQUIBASE_TABLE) && tableData.get(tableName) > 0)) {
                if (threadCount < MAX_THREADS) {
                    new Thread(new Runnable() {
                            public void run() {
                                threadCount++;
                                migrate(source, target, tableName, observable);
                                threadCount--;
                            }
                        }).start();
                }
                else {
                    final Map<String,Integer> columns = new HashMap<String, Integer>();
                    migrate(source, target, tableName, observable);
                }
            }
        }
    }

    private void migrate(final RdbmsConfig source, 
                         final RdbmsConfig target, 
                         final String tableName, 
                         final ProgressObservable observable) {
        final Connection sourceDb = openConnection(source);
        final Connection targetDb = openConnection(target);
        source.setConnection(sourceDb);
        target.setConnection(targetDb);
        final Map<String, Integer> columns = getColumnMap(source, target, tableName);
        source.setConnection(null);
        target.setConnection(null);

        if (columns.size() < 1) {
            log("Columns are empty");
            return;
        }

        PreparedStatement toStatement = prepareStatement(targetDb, tableName, columns);
        Statement fromStatement = null;

        final boolean hasClob = columns.values().contains(Types.CLOB);


        try {
            fromStatement = sourceDb.createStatement();

            final ResultSet results = fromStatement.executeQuery(String.format(SELECT_ALL_QUERY, tableName));
            while (results.next()) {
                try {
                    toStatement.clearParameters();
                    
                    int i = 1;
                    for (String columnName : columns.keySet()) {
                        final Object value = results.getObject(columnName);
                        
                        if (value != null) {
                            try {
                                handleLob(toStatement, value, i);
                            }
                            catch (Exception e) {
                                System.err.println(String.format("Error processing %s.%s %s", tableName, columnName, columns.get(columnName)));
                                if (Clob.class.isAssignableFrom(value.getClass())) {
                                    System.err.println("Got exception trying to insert CLOB with length" + ((Clob) value).length());
                                }
                                e.printStackTrace();
                            }
                        }
                        else {
                            toStatement.setObject(i,value);
                        } 
                        i++;
                    }
                    
                    boolean retry = true;
                    while(retry) {
                        try {
                            toStatement.execute();
                            retry = false;
                        }
                        catch (SQLException sqle) {
                            retry = false;
                            if (sqle.getMessage().contains("ORA-00942")) {
                                log("Couldn't find " + tableName);
                                log("Tried insert statement " + getStatementBuffer(tableName, columns));
                                // sqle.printStackTrace();
                            }
                            else if (sqle.getMessage().contains("ORA-12519")) {
                                retry = true;
                                log("Tried insert statement " + getStatementBuffer(tableName, columns));
                                sqle.printStackTrace();
                            }
                            else if (sqle.getMessage().contains("IN or OUT")) {
                                log("Column count was " + columns.keySet().size());
                            }
                            else {
                                sqle.printStackTrace();
                            }
                        }
                    }
                    
                }
                finally {
                    observable.incrementRecord();
                }
            }
            results.close();
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
        finally {
            if (sourceDb != null) {
                try {
                    if (sourceDb.getMetaData().getDriverName().toLowerCase().contains("hsqldb")) {
                        Statement st = sourceDb.createStatement();
                        st.execute("CHECKPOINT"); 
                        st.close();
                    }
                    fromStatement.close();
                    sourceDb.close();
                }
                catch (Exception e) {
                }
            }

            if (targetDb != null) {
                try {
                    targetDb.commit();
                    if (targetDb.getMetaData().getDriverName().toLowerCase().contains("hsqldb")) {
                        Statement st = targetDb.createStatement();
                        st.execute("CHECKPOINT"); 
                        st.close();
                    }
                    toStatement.close();
                    targetDb.close();
                }
                catch (Exception e) {
                    log("Error closing database connection");
                    e.printStackTrace();
                }
            }
            columns.clear();
        }
    }

    protected void handleLob(final PreparedStatement toStatement, final Object value, final int i) throws SQLException {
        if (Clob.class.isAssignableFrom(value.getClass())) {
            toStatement.setAsciiStream(i, ((Clob) value).getAsciiStream(), ((Clob) value).length());
        }
        else if (Blob.class.isAssignableFrom(value.getClass())) {
            toStatement.setBinaryStream(i, ((Blob) value).getBinaryStream(), ((Blob) value).length());
        }
        else {
            toStatement.setObject(i,value);
        } 
    }

    protected PreparedStatement prepareStatement(Connection conn, String tableName, Map<String, Integer> columns) {
        final String statement = getStatementBuffer(tableName, columns);
        
        try {
            return conn.prepareStatement(statement);
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
    }

    private String getStatementBuffer(String tableName, Map<String,Integer> columns) {
        String retval = null;

        final StringBuilder names  = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        for (String columnName : columns.keySet()) {
            names.append(columnName).append(",");
            values.append("?,");
        }

        names.setLength(names.length() - 1);
        values.setLength(values.length() - 1);
        retval = String.format(INSERT_STATEMENT, tableName, names, values);
        

        return retval;
    }

    /**
     * Get a list of table names available mapped to row counts
     */
    private Map<String, Integer> getTableData(RdbmsConfig source, RdbmsConfig target, Incrementor incrementor) {
        Connection sourceConn = openConnection(source);
        Connection targetConn = openConnection(target);
        final Map<String, Integer> retval = new HashMap<String, Integer>();
        final Collection<String> toRemove = new ArrayList<String>();

        debug("Looking up table names");
        
        try {
            final ResultSet tableResults = sourceConn.getMetaData().getTables(null, source.getSchema(), null, new String[] { "TABLE" });
            
            while (tableResults.next()) {
                final String tableName = tableResults.getString("TABLE_NAME");
                if (tableName.toUpperCase().startsWith(LIQUIBASE_TABLE)) continue;
                final int rowCount = getTableRecordCount(sourceConn, tableName);
                incrementor.increment(rowCount);
                retval.put(tableName, rowCount);
            }
            tableResults.close();
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
        finally {
            if (sourceConn != null) {
                try {
                    sourceConn.close();
                    sourceConn = null;
                }
                catch (Exception e) {
                }
            }
        }

        try {
            for (String tableName : retval.keySet()) {
                final ResultSet tableResults = targetConn.getMetaData().getTables(null, target.getSchema(), null, new String[] { "TABLE" });
                if (!tableResults.next()) {
                    toRemove.add(tableName);
                }
                tableResults.close();
            }
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
        finally {
            if (targetConn != null) {
                try {
                    targetConn.close();
                    targetConn = null;
                }
                catch (Exception e) {
                }
            }
        }

        for (String tableName : toRemove) {
            retval.remove(tableName);
        }
        
        return retval;
    }

    private Map<String, Integer> getColumnMap(final RdbmsConfig source, final RdbmsConfig target, String tableName) {
        final Connection targetDb = target.getConnection();
        final Connection sourceDb = source.getConnection();
        final Map<String,Integer> retval = new HashMap<String,Integer>();
        final Collection<String> toRemove = new ArrayList<String>();

        try {
            final ResultSet columnResults = targetDb.getMetaData().getColumns(null, target.getSchema(), tableName.toUpperCase(), null);
            while (columnResults.next()) {
                retval.put(columnResults.getString("COLUMN_NAME"),
                           columnResults.getInt("DATA_TYPE"));
            }
            columnResults.close();
        }
        catch (Exception e) {
            throw new BuildException(e);
        }

        for (final String column : retval.keySet()) {
            try {
                final ResultSet columnResults = sourceDb.getMetaData().getColumns(null, source.getSchema(), tableName, column);
                if (!columnResults.next()) {
                    toRemove.add(column);
                }
                columnResults.close();
            }
            catch (Exception e) {
                throw new BuildException(e);
            }
        }

        for (final String column : toRemove) {
            retval.remove(column);
        }
        
        return retval;
    }

    private int getTableRecordCount(Connection conn, String tableName) {
        final String query = String.format(RECORD_COUNT_QUERY, tableName);
        Statement statement = null;
        try {
            statement = conn.createStatement();
            final ResultSet results = statement.executeQuery(query);
            results.next();
            final int retval = results.getInt(COUNT_FIELD);
            results.close();
            return retval;
        }
        catch (Exception e) {
            if (e.getMessage().contains("ORA-00942")) {
                log("Couldn't find " + tableName);
                log("Tried insert statement " + query);
            }

            throw new BuildException(e);
        }
        finally {
            try {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
            }
            catch (Exception e) {
            }
        }
    }

    private void debug(String msg) {
        log(msg, MSG_DEBUG);
    }

    private Connection openSource() {
        return openConnection(getSource());
    }

    private Connection openTarget() {
        return openConnection(getTarget());
    }

    private Connection openConnection(String reference) {
        final RdbmsConfig config = (RdbmsConfig) getProject().getReference(reference);
        return openConnection(config);
    }
    
    private Connection openConnection(RdbmsConfig config) {
        Connection retval = null;
        
        while (retval == null) {
            try {
                debug("Loading schema " + config.getSchema() + " at url " + config.getUrl());
                Class.forName(config.getDriver());

                retval = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
                retval.setAutoCommit(false);


                // If this is an HSQLDB database, then we probably want to turn off logging for permformance
                if (config.getDriver().contains("hsqldb")) {
                    final Statement st = retval.createStatement();
                    st.execute("SET FILES LOG FALSE");
                    st.close();
                }
                
                return retval;
            }
            catch (Exception e) {
                // throw new BuildException(e);
            }
        }
        
        return retval;
    }

    /**
     * Helper class for incrementing values
     */
    private class Incrementor {
        private int value;
        
        public Incrementor() {
            value = 0;
        }
        
        public int getValue() {
            return value;
        }

        public void increment() {
            value++;
        }

        public void increment(int by) {
            value += by;
        }
    }

    private class ProgressObservable extends Observable {
        public void incrementRecord() {
            setChanged();
            notifyObservers();
            clearChanged();
        }
    }

    /**
     * Observer for handling progress
     * 
     */
    private class ProgressObserver implements Observer {
        private float total;
        private float progress;
        private float length;
        private float ratio;
        private String template;
        private float count;
        private PrintStream out;
        
        public ProgressObserver(final float total,
                                final float length,
                                final float ratio,
                                final String template) {
            this.total    = total;
            this.template = template;
            this.ratio    = ratio;
            this.length   = length;
            this.count    = 0;

            try {
                final Field field = Main.class.getDeclaredField("out");
                field.setAccessible(true);
                out = (PrintStream) field.get(null);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public synchronized void update(Observable o, Object arg) {
            count++;

            final int percent = (int) ((count / total) * 100f);
            final int progress = (int) ((count / total) * (100f * ratio));
            final StringBuilder progressBuffer = new StringBuilder();
                
            for (int x = 0; x < progress; x++) {
                progressBuffer.append('=');
            }
            
            for (int x = progress; x < length; x++) {
                progressBuffer.append(' ');
            }
            
            out.print(String.format(template, progressBuffer, percent, (int) count, (int) total));
            /*
            if ((count % 5000) == 0 || count == total) {
                out.print(String.format("(%s)%% %s of %s records", (int) ((count / total) * 100), (int) count, (int) total));
                }
            */
        }
    }
}