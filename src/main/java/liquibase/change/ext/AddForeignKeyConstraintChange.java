package liquibase.change.ext;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.database.Database;
import liquibase.database.structure.ForeignKeyConstraintType;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddForeignKeyConstraintStatement;

/**
 * Adds a foreign key constraint to an existing column. Making resulting foreign keys ignore schema information
 *
 * Leo Przybylski (leo [at] rsmart.com) 
 */
public class AddForeignKeyConstraintChange extends liquibase.change.core.AddForeignKeyConstraintChange {
    public SqlStatement[] generateStatements(Database database) {

        boolean deferrable = false;
        if (getDeferrable() != null) {
            deferrable = getDeferrable();
        }

        boolean initiallyDeferred = false;
        if (getInitiallyDeferred() != null) {
            initiallyDeferred = getInitiallyDeferred();
        }

        return new SqlStatement[]{
                new AddForeignKeyConstraintStatement(getConstraintName(), null,
                                                     getBaseTableName(),
                                                     getBaseColumnNames(),
                                                     null,
                                                     getReferencedTableName(),
                                                     getReferencedColumnNames())
                .setDeferrable(deferrable)
                .setInitiallyDeferred(initiallyDeferred)
                .setOnUpdate(getOnUpdate())
                .setOnDelete(getOnDelete())
                .setReferencesUniqueColumn(getReferencesUniqueColumn())
        };
    }
}
