package liquibase.sqlgenerator.ext;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.ext.DescribeSequenceStatement;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Getting the current sequence value
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class DescribeSequenceGeneratorMySql extends AbstractSqlGenerator<DescribeSequenceStatement> {

    @Override
    public int getPriority() {
        return 100;
    }
    
    @Override
    public boolean supports(final DescribeSequenceStatement statement, final Database database) {
        return database instanceof MySQLDatabase;
    }

    @Override
    public ValidationErrors validate(DescribeSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(DescribeSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> list = new ArrayList<Sql>();
        list.add(new UnparsedSql("select max(id) as \"MAX\" from " + statement.getSequenceName()));
        list.addAll(Arrays.asList(sqlGeneratorChain.generateSql(statement, database)));

        return list.toArray(new Sql[list.size()]);

    }
}
