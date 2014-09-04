package org.kualigan.tools.ant.tasks;

import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.integration.ant.BaseLiquibaseTask;
import liquibase.util.StringUtils;
import org.apache.tools.ant.BuildException;

import java.util.ArrayList;
import java.util.List;

public class DropAllTask extends BaseLiquibaseTask {

    private String schemas;

    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
    }
    
    protected void executeWithLiquibaseClassloader() throws BuildException {
        Liquibase liquibase = null;
        try {
            liquibase = createLiquibase();
            boolean retry = true;
            while (retry) {
                try {
                    if (StringUtils.trimToNull(schemas) != null) {
                        final List<CatalogAndSchema> schemas = new ArrayList<CatalogAndSchema>() {{
                                for (final String schema : StringUtils.splitAndTrim(getSchemas(), ",")) {
                                    add(new CatalogAndSchema(null, schema));
                                }
                            }};
                        
                        liquibase.dropAll(schemas.toArray(new CatalogAndSchema[schemas.size()]));
                    } else {
                        liquibase.dropAll();
                    }
                    retry = false;
                }
                catch (Exception e2) {
                    log(e2.getMessage());
                    if (e2.getMessage().indexOf("ORA-02443") < 0 && e2.getCause() != null && retry) {
                        retry = (e2.getCause().getMessage().indexOf("ORA-02443") > -1);
                    }
                    
                    if (!retry) {
                        throw e2;
                    }
                    else {
                        log("Got ORA-2443. Retrying...");
                    }
                }
            }       
        } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            closeDatabase(liquibase);
        }
    }
}
