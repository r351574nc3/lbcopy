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

    @Override
    public int getPriority() {
        return 10;
    }
    
    @Override
    public boolean supports(final CreateSequenceStatement statement, final Database database) {
        return ("mysql".equals(database.getTypeName()));
    }

    public ValidationErrors validate(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    public Sql[] generateSql(CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> list = new ArrayList<Sql>();
        list.add(new UnparsedSql(String.format(CREATE_SEQUENCE_STATEMENT, statement.getSequenceName())));
        list.addAll(Arrays.asList(sqlGeneratorChain.generateSql(statement, database)));

        return list.toArray(new Sql[list.size()]);

    }
}
