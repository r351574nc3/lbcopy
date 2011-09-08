package liquibase.database.ext;

import liquibase.database.AbstractDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawSqlStatement;

/**
 * Overridden to replace all schema instances with "" when getting view definitions. The purpose is to make it schema non-specific
 *
 * @author Leo Przybylski (leo [at] rsmart.com
 */
public class MySQLDatabase extends liquibase.database.ext.MySQLDatabase {

    public int getPriority() {
        return 100;
    }

    @Override
    public String getViewDefinition(String schemaName, String viewName) throws DatabaseException {
        if (schemaName == null) {
            schemaName = convertRequestedSchemaToSchema(null);
        }
        String definition = (String) ExecutorService.getInstance().getExecutor(this).queryForObject(new GetViewDefinitionStatement(schemaName, viewName), String.class);
        if (definition == null) {
            return null;
        }
        final String schemaStr = String.format("`%s`.", database.getDefaultSchemaName());
        definition = definition.replace(schemaStr, "");
        return CREATE_VIEW_AS_PATTERN.matcher(definition).replaceFirst("");
    }
}
