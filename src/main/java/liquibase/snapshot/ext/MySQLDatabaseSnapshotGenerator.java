// Copyright 2011 Leo Przybylski. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
//    1. Redistributions of source code must retain the above copyright notice, this list of
//       conditions and the following disclaimer.
//
//    2. Redistributions in binary form must reproduce the above copyright notice, this list
//       of conditions and the following disclaimer in the documentation and/or other materials
//       provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation are those of the
// authors and should not be interpreted as representing official policies, either expressed
// or implied, of Leo Przybylski.
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
import java.math.BigInteger;
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

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 * Detect sequences created with the {@link MysqlSequenceGenerator} hack.
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class MySQLDatabaseSnapshotGenerator extends liquibase.snapshot.jvm.MySQLDatabaseSnapshotGenerator {

    public int getPriority(Database database) {
        return EXTENSION_PRIORITY;
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
                    // seq.setSchema(convertedSchemaName);
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
