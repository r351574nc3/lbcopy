package liquibase.change.ext;

import liquibase.change.*;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Overridden to remove schema dependent info
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class CreateIndexChange extends liquibase.change.core.CreateIndexChange implements ChangeWithColumns {

    public CreateIndexChange() {
        setPriority(100);
    }

    public String getSchemaName() {
        return null;
    }

    public SqlStatement[] generateStatements(Database database) {
        List<String> columns = new ArrayList<String>();
        for (ColumnConfig column : getColumns()) {
            columns.add(column.getName());
        }

	    return new SqlStatement[]{
			    new CreateIndexStatement(
					    getIndexName(),
					    getSchemaName(),
					    getTableName(),
					    this.isUnique(),
					    getAssociatedWith(),
					    columns.toArray(new String[getColumns().size()]))
					    .setTablespace(getTablespace())
	    };
    }
}
