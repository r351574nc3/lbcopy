package liquibase.change.ext;

import liquibase.change.*;
import liquibase.database.Database;
import liquibase.database.structure.type.DataType;
import liquibase.database.typeconversion.TypeConverter;
import liquibase.database.typeconversion.TypeConverterFactory;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.*;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new table.
 */
public class CreateTableChange extends liquibase.change.core.CreateTableChange {

    public CreateTableChange() {
        setPriority(200);
    }

    public void addColumn(ColumnConfig column) {
        super.addColumn(column);
    }

    public SqlStatement[] generateStatements(Database database) {
        final SqlStatement[] retval = super.generateStatements(database);
        return retval;
    }
}
