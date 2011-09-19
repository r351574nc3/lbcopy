package com.rsmart.kuali.tools.ant.tasks;

import liquibase.integration.ant.*;
import liquibase.Liquibase;
import liquibase.util.ui.UIFactory;
import org.apache.tools.ant.BuildException;

import java.io.Writer;

/**
 * Ant task for migrating a database forward.
 */
public class DatabaseUpdateTask extends BaseLiquibaseTask {
    private boolean dropFirst = false;

    public boolean isDropFirst() {
        return dropFirst;
    }

    public void setDropFirst(boolean dropFirst) {
        this.dropFirst = dropFirst;
    }

    @Override
    public void execute() throws BuildException {
        if (!shouldRun()) {
            return;
        }

        super.execute();

        Liquibase liquibase = null;
        try {
            liquibase = createLiquibase();

            if (isPromptOnNonLocalDatabase()
                    && !liquibase.isSafeToRunMigration()
                    && UIFactory.getInstance().getFacade().promptForNonLocalDatabase(liquibase.getDatabase())) {
                throw new BuildException("Chose not to run against non-production database");
            }

            Writer writer = createOutputWriter();
            if (writer == null) {
                if (isDropFirst()) {
                    liquibase.dropAll();
                }

                liquibase.update(getContexts());
            } else {
                if (isDropFirst()) {
                    throw new BuildException("Cannot dropFirst when outputting update SQL");
                }
                liquibase.update(getContexts(), writer);
                writer.flush();
                writer.close();
            }

        } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            closeDatabase(liquibase);
        }
    }

    protected Liquibase createLiquibase() throws Exception {
        ResourceAccessor antFO = new AntResourceAccessor(getProject(), classpath);
        ResourceAccessor fsFO = new FileSystemResourceAccessor();

        Database database = createDatabaseObject(getDriver(), getUrl(), getUsername(), getPassword(), getDefaultSchemaName(), getDatabaseClass());

        String changeLogFile = null;
        if (getChangeLogFile() != null) {
            changeLogFile = getChangeLogFile().trim();
        }
        Liquibase liquibase = new Liquibase(changeLogFile, new CompositeResourceAccessor(antFO, fsFO), database);
        liquibase.setCurrentDateTimeFunction(currentDateTimeFunction);
        for (Map.Entry<String, Object> entry : changeLogProperties.entrySet()) {
            liquibase.setChangeLogParameter(entry.getKey(), entry.getValue());
        }

        return liquibase;
    }
}
