package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.AddUniqueConstraintStatement;
import liquibase.util.StringUtils;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

public class AddUniqueConstraintGenerator extends liquibase.sqlgenerator.core.AddUniqueConstraintGenerator {

    @Override
    public int getPriority() {
        return EXTENSION_PRIORITY;
    }

    public Sql[] generateSql(AddUniqueConstraintStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

		String sql = null;
		if (statement.getConstraintName() == null) {
			sql = String.format("ALTER TABLE %s ADD UNIQUE (%s)"
					, database.escapeTableName(null, statement.getTableName())
					, database.escapeColumnNameList(statement.getColumnNames())
			);
		} else {
			sql = String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)"
					, database.escapeTableName(null, statement.getTableName())
					, database.escapeConstraintName(statement.getConstraintName())
					, database.escapeColumnNameList(statement.getColumnNames())
			);
		}
		if(database instanceof OracleDatabase) {
	        if (statement.isDeferrable() || statement.isInitiallyDeferred()) {
	            if (statement.isDeferrable()) {
	            	sql += " DEFERRABLE";
	            }

	            if (statement.isInitiallyDeferred()) {
	            	sql +=" INITIALLY DEFERRED";
	            }
	        }
            if (statement.isDisabled()) {
                sql +=" DISABLE";
            }
		}

        if (StringUtils.trimToNull(statement.getTablespace()) != null && database.supportsTablespaces()) {
            if (database instanceof MSSQLDatabase) {
                sql += " ON " + statement.getTablespace();
            } else if (database instanceof DB2Database
                || database instanceof SybaseASADatabase
                || database instanceof InformixDatabase) {
                ; //not supported
            } else {
                sql += " USING INDEX TABLESPACE " + statement.getTablespace();
            }
        }

        return new Sql[] {
                new UnparsedSql(sql)
        };

    }
}
