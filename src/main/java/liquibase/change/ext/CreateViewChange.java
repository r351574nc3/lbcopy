package liquibase.change.ext;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.SQLiteDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateViewStatement;
import liquibase.statement.core.DropViewStatement;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new view. I've revised it to ignore the schema information because we want to do have transferable schemas. Uses
 * a null for schema name. Hope this works.
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class CreateViewChange extends liquibase.change.core.CreateViewChange {
	public SqlStatement[] generateStatements(Database database) {
		List<SqlStatement> statements = new ArrayList<SqlStatement>();

		boolean replaceIfExists = false;
		if (getReplaceIfExists() != null && getReplaceIfExists()) {
			replaceIfExists = true;
		}

		if (!supportsReplaceIfExistsOption(database) && replaceIfExists) {
			statements.add(new DropViewStatement(null, getViewName()));
			statements.add(new CreateViewStatement(null, getViewName(), getSelectQuery(),
					false));
		} else {
			statements.add(new CreateViewStatement(null, getViewName(), getSelectQuery(),
					replaceIfExists));
		}

		return statements.toArray(new SqlStatement[statements.size()]);
	}
}
