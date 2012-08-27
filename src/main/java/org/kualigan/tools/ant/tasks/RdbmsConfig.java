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
package org.kualigan.tools.ant.tasks;

import java.sql.Connection;
import java.util.List;
import java.util.Vector;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

import static org.apache.tools.ant.Project.MSG_DEBUG;

/**
 * Shorthand form of an SVN Repository. Localizes information for credentials and url, and stores
 * the information in an easy-to-reference id.
 *
 *
 * @author $Author$
 * @version $Revision$
 */
public class RdbmsConfig extends Task {
    private String id;
    private String url;
    private String schema;
    private String username;
    private String password;
    private String driver;
    private Connection connection;
    
    public RdbmsConfig() {
    }
    
    /**
     * Gets the value of url
     *
     * @return the value of url
     */
    public final String getUrl() {
        return this.url;
    }
    
    /**
     * Sets the value of url
     *
     * @param argUrl Value to assign to this.url
     */
    public final void setUrl(final String argUrl) {
        this.url = argUrl;
    }

    
    /**
     * Gets the value of Connection
     *
     * @return the value of Connection
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

    /**
     * Gets the value of id
     *
     * @return the value of id
     */
    public final String getId() {
        return this.id;
    }
    
    /**
     * Sets the value of id
     *
     * @param argId Value to assign to this.id
     */
    public final void setId(final String argId) {
        this.id = argId;
    }

    /**
     * Gets the value of username
     *
     * @return the value of username
     */
    public final String getUsername() {
        return this.username;
    }

    /**
     * Sets the value of username
     *
     * @param argUsername Value to assign to this.username
     */
    public final void setUsername(final String argUsername) {
        this.username = argUsername;
    }

    /**
     * Gets the value of password
     *
     * @return the value of password
     */
    public final String getPassword() {
        return this.password;
    }
    
    /**
     * Sets the value of password
     *
     * @param argPassword Value to assign to this.password
     */
    public final void setPassword(final String argPassword) {
        this.password = argPassword;
    }

    private String getReferenceId() {
        return getProject().getName() + "." + getId();
    }

    /**
     * Gets the value of driver
     *
     * @return the value of driver
     */
    public final String getDriver() {
        return this.driver;
    }

    /**
     * Sets the value of driver
     *
     * @param argDriver Value to assign to this.driver
     */
    public final void setDriver(final String argDriver) {
        this.driver = argDriver;
    }

    /**
     * Gets the value of schema
     *
     * @return the value of schema
     */
    public final String getSchema() {
        return this.schema;
    }

    /**
     * Sets the value of schema
     *
     * @param argSchema Value to assign to this.schema
     */
    public final void setSchema(final String argSchema) {
        this.schema = argSchema;
    }


    public void execute() {
        debug("Saving rdbms reference " + getReferenceId());
        getProject().addReference(getReferenceId(), this);
        getProject().setProperty("driver", getDriver());
        getProject().setProperty("url", getUrl());
        getProject().setProperty("username", getUsername());
        getProject().setProperty("password", getPassword());
        System.setProperty("jdbc.drivers", System.getProperty("jdbc.drivers") + ":" + getDriver());
    }

    private void debug(String msg) {
        log(msg, MSG_DEBUG);
    }
}
