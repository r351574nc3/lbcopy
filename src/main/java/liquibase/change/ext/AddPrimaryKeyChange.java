package liquibase.change.ext;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.database.core.DB2Database;
import liquibase.database.core.SQLiteDatabase;
import liquibase.database.core.SQLiteDatabase.AlterTableVisitor;
import liquibase.database.structure.Index;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddPrimaryKeyStatement;
import liquibase.statement.core.ReorganizeTableStatement;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;


/**
 * Overridden to ignore schema information
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class AddPrimaryKeyChange extends liquibase.change.core.AddPrimaryKeyChange {
    
    public AddPrimaryKeyChange() {
        setPriority(100);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        String schemaName = null;

        AddPrimaryKeyStatement statement = new AddPrimaryKeyStatement(schemaName, getTableName(), getColumnNames(), getConstraintName());
        statement.setTablespace(getTablespace());

        if (database instanceof DB2Database) {
            return new SqlStatement[]{
                    statement,
                    new ReorganizeTableStatement(schemaName, getTableName())
            };
//todo        } else if (database instanceof SQLiteDatabase) {
//            // return special statements for SQLite databases
//            return generateStatementsForSQLiteDatabase(database);
        }

        return new SqlStatement[]{
                statement
        };
    }
}
