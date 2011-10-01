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
package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.logging.LogFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.ForeignKeyConstraint;
import liquibase.statement.UniqueConstraint;
import liquibase.statement.core.CreateTableStatement;
import liquibase.util.StringUtils;
import java.util.Iterator;
import java.math.BigDecimal;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class CreateTableGenerator extends liquibase.sqlgenerator.core.CreateTableGenerator {

    @Override
    public int getPriority() {
        return EXTENSION_PRIORITY;
    }
    
    @Override
    public ValidationErrors validate(CreateTableStatement createTableStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", createTableStatement.getTableName());
        validationErrors.checkRequiredField("columns", createTableStatement.getColumns());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
            StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE TABLE ").append(database.escapeTableName(null, statement.getTableName())).append(" ");
        buffer.append("(");
        
        boolean isSinglePrimaryKeyColumn = statement.getPrimaryKeyConstraint() != null
            && statement.getPrimaryKeyConstraint().getColumns().size() == 1;
        
        boolean isPrimaryKeyAutoIncrement = false;
        
        Iterator<String> columnIterator = statement.getColumns().iterator();
        while (columnIterator.hasNext()) {
            String column = columnIterator.next();
            
            buffer.append(database.escapeColumnName(null, statement.getTableName(), column));
            buffer.append(" ").append(statement.getColumnTypes().get(column));
            
            AutoIncrementConstraint autoIncrementConstraint = null;
            
            for (AutoIncrementConstraint currentAutoIncrementConstraint : statement.getAutoIncrementConstraints()) {
            	if (column.equals(currentAutoIncrementConstraint.getColumnName())) {
            		autoIncrementConstraint = currentAutoIncrementConstraint;
            		break;
            	}
            }

            boolean isAutoIncrementColumn = autoIncrementConstraint != null;            
            boolean isPrimaryKeyColumn = statement.getPrimaryKeyConstraint() != null
            		&& statement.getPrimaryKeyConstraint().getColumns().contains(column);
            isPrimaryKeyAutoIncrement = isPrimaryKeyAutoIncrement
            		|| isPrimaryKeyColumn && isAutoIncrementColumn;
            
            if ((database instanceof SQLiteDatabase) &&
					isSinglePrimaryKeyColumn &&
					isPrimaryKeyColumn &&
					isAutoIncrementColumn) {
            	String pkName = StringUtils.trimToNull(statement.getPrimaryKeyConstraint().getConstraintName());
	            if (pkName == null) {
	                pkName = database.generatePrimaryKeyName(statement.getTableName());
	            }
                if (pkName != null) {
                    buffer.append(" CONSTRAINT ");
                    buffer.append(database.escapeConstraintName(pkName));
                }
                buffer.append(" PRIMARY KEY AUTOINCREMENT");
			}

            if (statement.getDefaultValue(column) != null) {
                Object defaultValue = statement.getDefaultValue(column);
                
                if (statement.getColumnTypes().get(column).toString().startsWith("DECIMAL")
                    || statement.getColumnTypes().get(column).toString().startsWith("NUMERIC")) {
                    int[] bounds = parseBounds(statement.getColumnTypes().get(column).toString());
                    BigDecimal parsedValue = new BigDecimal(defaultValue.toString());
                    
                    StringBuilder max = new StringBuilder();
                    for (int i = 0; i < bounds[0] - bounds[1]; i++) max.append("9");
                    
                    if (bounds[1] > 0) {
                        max.append(".");
                    }
                    for (int i = 0; i < bounds[1]; i++) max.append("9");
                    
                    if (parsedValue.compareTo(new BigDecimal(max.toString())) > 0) {
                        defaultValue = max;
                    }
                }
                if (database instanceof MSSQLDatabase) {
                    buffer.append(" CONSTRAINT ").append(((MSSQLDatabase) database).generateDefaultConstraintName(statement.getTableName(), column));
                }
                buffer.append(" DEFAULT ");
                buffer.append(statement.getColumnTypes().get(column).convertObjectToString(defaultValue, database));
            }

            if (isAutoIncrementColumn) {
            	// TODO: check if database supports auto increment on non primary key column
                if (database.supportsAutoIncrement()) {
                	String autoIncrementClause = database.getAutoIncrementClause(autoIncrementConstraint.getStartWith(), autoIncrementConstraint.getIncrementBy());
                
                	if (!"".equals(autoIncrementClause)) {
                		buffer.append(" ").append(autoIncrementClause);
                	}
                } else {
                    LogFactory.getLogger().warning(database.getTypeName()+" does not support autoincrement columns as request for "+(database.escapeTableName(null, statement.getTableName())));
                }
            }

            if (statement.getNotNullColumns().contains(column)) {
                buffer.append(" NOT NULL");
            } else {
                if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase) {
                    buffer.append(" NULL");
                }
            }

            if (database instanceof InformixDatabase && isSinglePrimaryKeyColumn) {
            	buffer.append(" PRIMARY KEY");
            }

            if (columnIterator.hasNext()) {
                buffer.append(", ");
            }
        }

        buffer.append(",");

        // TODO informixdb
        if (!( (database instanceof SQLiteDatabase) &&
				isSinglePrimaryKeyColumn &&
				isPrimaryKeyAutoIncrement) &&

				!((database instanceof InformixDatabase) &&
				isSinglePrimaryKeyColumn
				)) {
        	// ...skip this code block for sqlite if a single column primary key
        	// with an autoincrement constraint exists.
        	// This constraint is added after the column type.

	        if (statement.getPrimaryKeyConstraint() != null && statement.getPrimaryKeyConstraint().getColumns().size() > 0) {
	        	if (!(database instanceof InformixDatabase)) {
		            String pkName = StringUtils.trimToNull(statement.getPrimaryKeyConstraint().getConstraintName());
		            if (pkName == null) {
		                // TODO ORA-00972: identifier is too long
			            // If tableName lenght is more then 28 symbols
			            // then generated pkName will be incorrect
			            pkName = database.generatePrimaryKeyName(statement.getTableName());
		            }
                    if (pkName != null) {
                        buffer.append(" CONSTRAINT ");
                        buffer.append(database.escapeConstraintName(pkName));
                    }
                }
	            buffer.append(" PRIMARY KEY (");
	            buffer.append(database.escapeColumnNameList(StringUtils.join(statement.getPrimaryKeyConstraint().getColumns(), ", ")));
	            buffer.append(")");
		        // Setting up table space for PK's index if it exist
		        if (database instanceof OracleDatabase &&
		            statement.getPrimaryKeyConstraint().getTablespace() != null) {
			        buffer.append(" USING INDEX TABLESPACE ");
			        buffer.append(statement.getPrimaryKeyConstraint().getTablespace());
		        }
	            buffer.append(",");
	        }
        }

        for (ForeignKeyConstraint fkConstraint : statement.getForeignKeyConstraints()) {
        	if (!(database instanceof InformixDatabase)) {
        		buffer.append(" CONSTRAINT ");
                buffer.append(database.escapeConstraintName(fkConstraint.getForeignKeyName()));
        	}
            String referencesString = fkConstraint.getReferences();
            buffer.append(" FOREIGN KEY (")
                    .append(database.escapeColumnName(null, statement.getTableName(), fkConstraint.getColumn()))
                    .append(") REFERENCES ")
                    .append(referencesString);

            if (fkConstraint.isDeleteCascade()) {
                buffer.append(" ON DELETE CASCADE");
            }

            if ((database instanceof InformixDatabase)) {
            	buffer.append(" CONSTRAINT ");
            	buffer.append(database.escapeConstraintName(fkConstraint.getForeignKeyName()));
            }

            if (fkConstraint.isInitiallyDeferred()) {
                buffer.append(" INITIALLY DEFERRED");
            }
            if (fkConstraint.isDeferrable()) {
                buffer.append(" DEFERRABLE");
            }
            buffer.append(",");
        }

        for (UniqueConstraint uniqueConstraint : statement.getUniqueConstraints()) {
            if (uniqueConstraint.getConstraintName() != null && !constraintNameAfterUnique(database)) {
                buffer.append(" CONSTRAINT ");
                buffer.append(database.escapeConstraintName(uniqueConstraint.getConstraintName()));
            }
            buffer.append(" UNIQUE (");
            buffer.append(database.escapeColumnNameList(StringUtils.join(uniqueConstraint.getColumns(), ", ")));
            buffer.append(")");
            if (uniqueConstraint.getConstraintName() != null && constraintNameAfterUnique(database)) {
                buffer.append(" CONSTRAINT ");
                buffer.append(database.escapeConstraintName(uniqueConstraint.getConstraintName()));
            }
            buffer.append(",");
        }

//        if (constraints != null && constraints.getCheck() != null) {
//            buffer.append(constraints.getCheck()).append(" ");
//        }
//    }

        String sql = buffer.toString().replaceFirst(",\\s*$", "") + ")";

//        if (StringUtils.trimToNull(tablespace) != null && database.supportsTablespaces()) {
//            if (database instanceof MSSQLDatabase) {
//                buffer.append(" ON ").append(tablespace);
//            } else if (database instanceof DB2Database) {
//                buffer.append(" IN ").append(tablespace);
//            } else {
//                buffer.append(" TABLESPACE ").append(tablespace);
//            }
//        }

        if (statement.getTablespace() != null && database.supportsTablespaces()) {
            if (database instanceof MSSQLDatabase || database instanceof SybaseASADatabase) {
                sql += " ON " + statement.getTablespace();
            } else if (database instanceof DB2Database || database instanceof InformixDatabase) {
                sql += " IN " + statement.getTablespace();
            } else {
                sql += " TABLESPACE " + statement.getTablespace();
            }
        }

        return new Sql[] {
                new UnparsedSql(sql)
        };
    }

    private boolean constraintNameAfterUnique(Database database) {
		return database instanceof InformixDatabase;
	}

    
    protected int[] parseBounds(final String decimal) {
        final int[] retval = new int[2];
        int comma = decimal.indexOf(",");

        try {
            if (comma < 0) {
                retval[0] = Integer.parseInt(decimal.substring(decimal.indexOf("(") + 1, decimal.length() - 2));
                retval[1] = 0;
            }
            else {
                retval[0] = Integer.parseInt(decimal.substring(decimal.indexOf("(") + 1, comma));
                retval[1] = Integer.parseInt(comma + 1, decimal.indexOf(")")));
            }
        }
        catch (StringIndexOutOfBoundsException e) {
            System.out.println("parsebounds " + decimal);
            throw e;
        }
        return retval;
    }
}
