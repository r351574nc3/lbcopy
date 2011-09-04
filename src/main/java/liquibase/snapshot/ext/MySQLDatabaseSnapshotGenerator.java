package liquibase.snapshot.ext;

import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.database.typeconversion.TypeConverterFactory;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.structure.*;
import liquibase.exception.DatabaseException;
import liquibase.executor.*;
import liquibase.snapshot.*;
import liquibase.servicelocator.ServiceLocator;
import liquibase.sqlgenerator.SqlGenerator;
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

/**
 * Detect sequences created with the {@link MysqlSequenceGenerator} hack.
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class MySQLDatabaseSnapshotGenerator extends liquibase.snapshot.jvm.MySQLDatabaseSnapshotGenerator {

    protected void readSequences(final DatabaseSnapshot snapshot,                                  
                                 final String schema, 
                                 final DatabaseMetaData databaseMetaData) throws DatabaseException {
        final Database database = snapshot.getDatabase();

        updateListeners("Reading sequences for " + database.toString() + " ...");

        final String convertedSchemaName = database.convertRequestedSchemaToSchema(schema);
        try {
            final ResultSet rs = databaseMetaData.getTables(null, convertedSchemaName, null, new String[] { "TABLE" });
            while (rs.next()) {
                final String sequenceName = rs.getString("TABLE_NAME");
                if (isSequence(sequenceName, databaseMetaData)) {
                    final Sequence seq = new Sequence();
                    seq.setName(sequenceName.trim());
                    seq.setSchema(convertedSchemaName);
                    
                    snapshot.getSequences().add(seq);
                }
            }   
        }
        catch (SQLException sqle) {
            throw new DatabaseException(sqle);
        }
    }
    
    protected boolean isSequence(final String tableName, final DatabaseMetaData databaseMetaData) throws SQLException {
        final ResultSet rs = databaseMetaData.getColumns(null, null, tableName, null);
        Integer count = 0;
        boolean hasId = false;
        while (rs.next()) {
            count++;
            hasId = rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes");
        }

        return hasId && count == 1;
    }
}
