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

import org.apache.tools.ant.taskdefs.Jar;
import org.apache.tools.ant.types.FileSet;

import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.Liquibase;
import liquibase.integration.ant.AntResourceAccessor;
import liquibase.integration.ant.BaseLiquibaseTask;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.core.H2Database;
import liquibase.database.jvm.JdbcConnection;
import org.apache.tools.ant.BuildException;

import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;

import com.rsmart.kuali.tools.liquibase.Diff;
import com.rsmart.kuali.tools.liquibase.DiffResult;

import java.io.File;
import java.io.PrintStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.apache.tools.ant.Project.MSG_DEBUG;

/**
 *
 * @author Leo Przybylski (przybyls@arizona.edu)
 */
public class GenerateChangeLog extends BaseLiquibaseTask {
    private String source;
    private String target;
    private boolean stateSaved;

    public GenerateChangeLog() { }

    public boolean isStateSaved() {
        return stateSaved;
    }

    public void setStateSaved(boolean ss) {
        stateSaved = ss;
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
        Database lbSource = null;
        Database lbTarget = null;
        final DatabaseFactory factory = DatabaseFactory.getInstance();
        try {
            lbSource = factory.findCorrectDatabaseImplementation(new JdbcConnection(openConnection("source")));
            lbSource.setDefaultSchemaName(source.getSchema());
            lbTarget = factory.findCorrectDatabaseImplementation(new JdbcConnection(openConnection("target")));
            lbTarget.setDefaultSchemaName(target.getSchema());
            
            exportSchema(lbSource, lbTarget);
            if (isStateSaved()) {
                exportData(lbSource, lbTarget);
            }
            
            if (lbTarget instanceof H2Database) {
                final Statement st = ((JdbcConnection) lbTarget.getConnection()).createStatement();
                st.execute("SHUTDOWN DEFRAG");
            }
            
        } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            try {
                if (lbSource != null) {
                    lbSource.close();
                }
                if (lbTarget != null) {
                    lbTarget.close();
                }
            }
            catch (Exception e) {
            }
        }

        if (isStateSaved()) {
            log("Starting data load from schema " + source.getSchema());
            MigrateData migrateTask = new MigrateData();
            migrateTask.bindToOwner(this);
            migrateTask.init();
            migrateTask.setSource(getSource());
            migrateTask.setTarget("h2");
            migrateTask.execute();
            try {
                Backup.execute("work/export/data.zip", "work/export", "", true);
                
                // delete the old database files
                DeleteDbFiles.execute("split:22:work/export", "data", true);
            }
            catch (Exception e) {
                throw new BuildException(e);
            }
        }
    }

    protected void exportSchema(Database source, Database target) {
        try {
            Diff diff = new Diff(source, source.getDefaultSchemaName());
            exportTables(diff, target);
            exportSequences(diff, target);
            exportViews(diff, target);
            exportIndexes(diff, target);
            exportConstraints(diff, target);
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
    }

    protected void export(Diff diff, Database target, String diffTypes, String suffix) {
        diff.setDiffTypes(diffTypes);
        
        try {
            DiffResult results = diff.compare();
            results.printChangeLog(getChangeLogFile() + suffix, target);
        } 
        catch (Exception e) {
            throw new BuildException(e);
        }
    }

    protected void exportConstraints(Diff diff, Database target) {
        export(diff, target, "foreignKeys", "-cst.xml");
    }

    protected void exportIndexes(Diff diff, Database target) {
        export(diff, target, "indexes", "-idx.xml");
    }

    protected void exportViews(Diff diff, Database target) {
        export(diff, target, "views", "-vw.xml");
    }

    protected void exportTables(Diff diff, Database target) {
        export(diff, target, "tables, primaryKeys, uniqueConstraints", "-tab.xml");
    }

    protected void exportSequences(Diff diff, Database target) {
        export(diff, target, "sequences", "-seq.xml");
    }


    private void exportData(Database source, Database target) {
        Database h2db = null;
        RdbmsConfig h2Config = new RdbmsConfig();
        h2Config.setDriver("org.h2.Driver");
        h2Config.setUrl("jdbc:h2:split:22:work/export/data");
        h2Config.setUsername("SA");
        h2Config.setPassword("");
        h2Config.setSchema("PUBLIC");
        getProject().addReference("h2", h2Config);
        
        final DatabaseFactory factory = DatabaseFactory.getInstance();
        try {
            h2db = factory.findCorrectDatabaseImplementation(new JdbcConnection(openConnection("h2")));
            h2db.setDefaultSchemaName(h2Config.getSchema());
            
            export(new Diff(source, getDefaultSchemaName()), h2db, "tables", "-dat.xml");

            ResourceAccessor antFO = new AntResourceAccessor(getProject(), classpath);
            ResourceAccessor fsFO = new FileSystemResourceAccessor();
            
            String changeLogFile = getChangeLogFile() + "-dat.xml";

            Liquibase liquibase = new Liquibase(changeLogFile, new CompositeResourceAccessor(antFO, fsFO), h2db);

            log("Loading Schema");
            liquibase.update(getContexts());
            log("Finished Loading the Schema");

        } 
        catch (Exception e) {
            throw new BuildException(e);
        } 
        finally {
            try {
                if (h2db != null) {
                    // hsqldb.getConnection().createStatement().execute("SHUTDOWN");
                    log("Closing h2 database");
                    h2db.close();
                }
            }
            catch (Exception e) {
                if (!(e instanceof java.sql.SQLNonTransientConnectionException)) {
                    e.printStackTrace();
                }
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
        int retry_count = 0;
        final int max_retry = 5;
        while (retry_count < max_retry) {
            try {
                debug("Loading schema " + config.getSchema() + " at url " + config.getUrl());
                Class.forName(config.getDriver());
                retval = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
                retval.setAutoCommit(true);
            }
            catch (Exception e) {
                if (!e.getMessage().contains("Database lock acquisition failure") && !(e instanceof NullPointerException)) {
                    throw new BuildException(e);
                }
            }
            finally {
                retry_count++;
            }
        }
        return retval;
    }
}