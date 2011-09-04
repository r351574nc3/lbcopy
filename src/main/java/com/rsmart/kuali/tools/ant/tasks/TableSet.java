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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.tools.ant.BuildException;

/**
 *
 * @author Leo Przybylski (przybyls@arizona.edu)
 */
public class TableSet {
    private Vector<Include> includes;
    private Set<String> tables;
    private Connection connection;
    private String schema;

    public TableSet() {
        includes = new Vector();
        tables = new HashSet();
    }
    

    public Include createInclude() {
        Include retval = new Include();
        includes.add(retval);
        return retval;
    }

    public void execute() {
        try {
            DatabaseMetaData metadata = getConnection().getMetaData();
            System.out.printf("Looking for tables in schema %s\n", getSchema());
            ResultSet rs = metadata.getTables(null, getSchema(), null, new String[] {"TABLE"});

            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (isTableNameValid(tableName)) {
                    tables.add(tableName);
                }
            }
        }
        catch (Exception e) {
            throw new BuildException("Exception when getting table names", e);
        }
    }

    private boolean isTableNameValid(String tableName) {
        boolean retval = includes.size() == 0;
        for (Include include : includes) {
            retval |= Pattern.compile(include.getRegex()).matcher(tableName).matches();
        }
        return retval;        
    }

    public Set<String> getTables() {
        return tables;
    }
    
     
    /**
     * Gets the value of connection
     *
     * @return the value of connection
     */
    public final Connection getConnection() {
        return this.connection;
    }
    
    /**
     * Sets the value of connection
     *
     * @param argConnection Value to assign to this.connection
     */
    public final void setConnection(final Connection argConnection) {
        this.connection = argConnection;
    }


    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }
}