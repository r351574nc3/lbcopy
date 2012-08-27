// Copyright 2011 Leo Przybylski. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
//    1. Redistributions of source code must retain the above copyright notice, this list of
//       conditions and the following disclaimer.
//
//    2. Redistributions in binary form must reproduce the above copyright notice, this list
//       of conditions and the following disclaimer in the documentation and/or other materials
//       provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY Leo Przybylski ''AS IS'' AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation are those of the
// authors and should not be interpreted as representing official policies, either expressed
// or implied, of Leo Przybylski.
package org.kualigan.tools.ant.tasks;

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

import org.kualigan.tools.liquibase.Diff;
import org.kualigan.tools.liquibase.DiffResult;

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