package liquibase.database.ext;

import liquibase.exception.DatabaseException;

/**
 * Overridden to replace all schema instances with "" when getting view definitions. The purpose is to make it schema non-specific
 *
 * @author Leo Przybylski (leo [at] rsmart.com
 */
public class MySqlDatabase extends liquibase.database.core.MySQLDatabase {

    public int getPriority() {
        return 200;
    }

    @Override
    public String getViewDefinition(String schemaName, String viewName) throws DatabaseException {
        String retval = super.getViewDefinition(schemaName, viewName);
        final String schemaStr = String.format("`%s`.", schemaName);
        retval = retval.replaceAll(schemaStr.toLowerCase(), "");
        retval = retval.replaceAll(schemaStr.toUpperCase(), "");
        return retval;
    }
}
