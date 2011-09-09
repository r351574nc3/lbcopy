package liquibase.snapshot.ext;

import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.database.typeconversion.TypeConverterFactory;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.structure.*;
import liquibase.diff.DiffStatusListener;
import liquibase.exception.DatabaseException;
import liquibase.executor.*;
import liquibase.snapshot.*;
import liquibase.servicelocator.ServiceLocator;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.statement.core.SelectSequencesStatement;
import liquibase.statement.SqlStatement;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detect sequences created with the {@link MysqlSequenceGenerator} hack.
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class MySQLDatabaseSnapshotGenerator extends liquibase.snapshot.jvm.MySQLDatabaseSnapshotGenerator {

    public int getPriority(Database database) {
        return 300;
    }

    protected View readView(final String name, final Database database) throws DatabaseException {
        final String schemaName = convertFromDatabaseName(database.getDefaultSchemaName());
        View view = new View();
        view.setName(name);
        view.setSchema(null);
        view.setRawSchemaName(null);
        view.setRawCatalogName(database.getDefaultCatalogName());
        try {
            view.setDefinition(database.getViewDefinition(schemaName, name));
        } catch (DatabaseException e) {
            throw new DatabaseException("Error getting " + database.getConnection().getURL() + " view with " + new GetViewDefinitionStatement(view.getSchema(), name), e);
        }

        return view;
    }

    protected void readViews(DatabaseSnapshot snapshot, String schema, DatabaseMetaData databaseMetaData) throws SQLException, DatabaseException {
        Database database = snapshot.getDatabase();
        updateListeners("Reading views for " + database.toString() + " ...");
        
        ResultSet rs = databaseMetaData.getTables(database.convertRequestedSchemaToCatalog(schema), database.convertRequestedSchemaToSchema(schema), null, new String[]{"VIEW"});
        try {
            while (rs.next()) {
                final String name = convertFromDatabaseName(rs.getString("TABLE_NAME"));
                final View view = readView(name, database);
                if (database.isSystemView(view.getRawCatalogName(), view.getRawSchemaName(), view.getName())) {
                    continue;
                }
                
                snapshot.getViews().add(view);
            }
        } finally {
            try {
                rs.close();
            } catch (SQLException ignore) { }
        }
    }
}
