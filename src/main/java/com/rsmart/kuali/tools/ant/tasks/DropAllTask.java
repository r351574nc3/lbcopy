package com.rsmart.kuali.tools.ant.tasks;

import liquibase.Liquibase;
import liquibase.integration.ant.BaseLiquibaseTask;
import liquibase.util.StringUtils;
import org.apache.tools.ant.BuildException;

import java.util.List;

public class DropAllTask extends BaseLiquibaseTask {

    private String schemas;

    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
    }

    public void execute() throws BuildException {
        Liquibase liquibase = null;
        try {
       } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            closeDatabase(liquibase);
        }
    }
}