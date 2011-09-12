package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.CreateSequenceStatement;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Sequence hack for MySQL
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class MysqlSequenceGenerator extends AbstractSqlGenerator<CreateSequenceStatement> {
    private static final String CREATE_SEQUENCE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (id bigint(19) NOT NULL auto_increment, PRIMARY KEY(id) )";
    private static final String SET_START_VALUE_STATEMENT = "INSERT INTO %s VALUES (%s)";

    @Override
    public int getPriority() {
        return 100;
    }
    
    @Override
    public boolean supports(final CreateSequenceStatement statement, final Database database) {
        return ("mysql".equals(database.getTypeName()));
    }

    @Override
    public ValidationErrors validate(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> list = new ArrayList<Sql>();
        list.add(new UnparsedSql(String.format(CREATE_SEQUENCE_STATEMENT, statement.getSequenceName())));
        /* This is already taken care of during data migration
        if (statement.getStartValue() != null) {
            // System.out.println("Got start value " + statement.getStartValue());
            list.add(new UnparsedSql(String.format(SET_START_VALUE_STATEMENT, statement.getSequenceName(), statement.getStartValue())));
        }
        */
        list.addAll(Arrays.asList(sqlGeneratorChain.generateSql(statement, database)));

        return list.toArray(new Sql[list.size()]);

    }
}
