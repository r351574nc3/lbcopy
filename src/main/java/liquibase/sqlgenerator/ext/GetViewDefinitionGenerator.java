package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.GetViewDefinitionStatement;

/**
 * Overridden to replace all schema instances with "". The purpose is to make it schema non-specific
 *
 * @author Leo Przybylski (leo [at] rsmart.com
 */
public class GetViewDefinitionGenerator extends liquibase.sqlgenerator.core.GetViewDefinitionGenerator {
    @Override
    public int getPriority() {
        return 100;
    }

    @Override
    public Sql[] generateSql(final GetViewDefinitionStatement statement, final Database database, final SqlGeneratorChain sqlGeneratorChain) {
        final Sql[] retval = super.generateSql(statement, database, sqlGeneratorChain);
        final String schemaStr = String.format("`%s`.", database.getDefaultSchemaName());

        retval[0] = new UnparsedSql(retval[0].toSql().replace(schemaStr, ""));

        return retval;
    }
}
