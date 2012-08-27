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

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

import static org.apache.tools.ant.Project.MSG_DEBUG;


/**
 * Sets up conventional liquibase properties
 *
 * @author Leo Przybylski (przybyls@arizona.edu)
 */
public class LiquibaseSetup extends Task {
    private String rdbmsConfigId;

    /**
     * Gets the value of rdbmsConfigId
     *
     * @return the value of rdbmsConfigId
     */
    public final String getRdbmsConfigId() {
        return this.rdbmsConfigId;
    }

    /**
     * Sets the value of rdbmsConfigId
     *
     * @param argRdbmsConfigId Value to assign to this.rdbmsConfigId
     */
    public final void setRdbmsConfigId(final String argRdbmsConfigId) {
        this.rdbmsConfigId = argRdbmsConfigId;
    }    

    public void execute() {
        final RdbmsConfig config = (RdbmsConfig) getProject().getReference(getRdbmsConfigId());
        getProject().setProperty("lb.database.url", config.getUrl());
        getProject().setProperty("lb.database.username", config.getUsername());
        getProject().setProperty("lb.database.password", config.getPassword());
    }
}