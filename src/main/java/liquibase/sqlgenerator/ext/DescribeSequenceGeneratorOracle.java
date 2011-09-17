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
import liquibase.database.core.OracleDatabase;
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

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 * Getting the current sequence value
 *
 * @author Leo Przybylski (leo [at] rsmart.com)
 */
public class DescribeSequenceGeneratorOracle extends AbstractSqlGenerator<DescribeSequenceStatement> {
    private static final String CREATE_SEQUENCE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (id bigint(19) NOT NULL auto_increment, PRIMARY KEY(id) )";
    private static final String SET_START_VALUE_STATEMENT = "INSERT INTO %s VALUES (%s)";

    @Override
    public int getPriority() {
        return EXTENSION_PRIORITY;
    }
    
    @Override
    public boolean supports(final DescribeSequenceStatement statement, final Database database) {
        return database instanceof OracleDatabase;
    }

    @Override
    public ValidationErrors validate(DescribeSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(DescribeSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> list = new ArrayList<Sql>();
        list.add(new UnparsedSql(String.format("select %s.nextval from dual", statement.getSequenceName())));
        list.addAll(Arrays.asList(sqlGeneratorChain.generateSql(statement, database)));

        return list.toArray(new Sql[list.size()]);

    }
}
