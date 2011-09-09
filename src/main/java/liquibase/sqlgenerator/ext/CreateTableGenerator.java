package liquibase.sqlgenerator.ext;

import liquibase.database.structure.type.DecimalType;
import liquibase.database.Database;
import liquibase.database.typeconversion.TypeConverterFactory;
import liquibase.database.core.*;
import liquibase.exception.ValidationErrors;
import liquibase.logging.LogFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.ForeignKeyConstraint;
import liquibase.statement.UniqueConstraint;
import liquibase.statement.core.CreateTableStatement;
import liquibase.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.Iterator;

/**
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class CreateTableGenerator extends liquibase.sqlgenerator.core.CreateTableGenerator {

    @Override
    public int getPriority() {
        return 100;
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
        Iterator<String> columnIterator = statement.getColumns().iterator();
        while (columnIterator.hasNext()) {
            String column = columnIterator.next();
            boolean isAutoIncrement = statement.getAutoIncrementColumns().contains(column);

            buffer.append(database.escapeColumnName(null, statement.getTableName(), column));
            buffer.append(" ").append(statement.getColumnTypes().get(column));

            if ((database instanceof SQLiteDatabase) &&
					(statement.getPrimaryKeyConstraint()!=null) &&
					(statement.getPrimaryKeyConstraint().getColumns().size()==1) &&
					(statement.getPrimaryKeyConstraint().getColumns().contains(column)) &&
					isAutoIncrement) {
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
                if (statement.getColumnTypes().get(column).toString().startsWith("DECIMAL")) {
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

            if (isAutoIncrement &&
					(database.getAutoIncrementClause()!=null) &&
					(!database.getAutoIncrementClause().equals(""))) {
                if (database.supportsAutoIncrement()) {
                    buffer.append(" ").append(database.getAutoIncrementClause()).append(" ");
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

            if ((database instanceof InformixDatabase) &&
					(statement.getPrimaryKeyConstraint()!=null) &&
					(statement.getPrimaryKeyConstraint().getColumns().size()==1) &&
					(statement.getPrimaryKeyConstraint().getColumns().contains(column))) {
            	buffer.append(" PRIMARY KEY");
            }

            if (columnIterator.hasNext()) {
                buffer.append(", ");
            }
        }

        buffer.append(",");

        // TODO informixdb
        if (!( (database instanceof SQLiteDatabase) &&
				(statement.getPrimaryKeyConstraint()!=null) &&
				(statement.getPrimaryKeyConstraint().getColumns().size()==1) &&
				statement.getAutoIncrementColumns().contains(statement.getPrimaryKeyConstraint().getColumns().get(0)) ) &&

				!((database instanceof InformixDatabase) &&
				(statement.getPrimaryKeyConstraint()!=null) &&
				(statement.getPrimaryKeyConstraint().getColumns().size()==1)
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
        retval[0] = Integer.parseInt(decimal.substring(decimal.indexOf("(") + 1, decimal.indexOf(",")));
        retval[1] = Integer.parseInt(decimal.substring(decimal.indexOf(",") + 1, decimal.indexOf(")")));
        return retval;
    }
}
