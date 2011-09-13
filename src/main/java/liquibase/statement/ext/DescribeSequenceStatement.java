package liquibase.statement.ext;

import liquibase.statement.AbstractSqlStatement;

import java.math.BigInteger;

public class DescribeSequenceStatement extends AbstractSqlStatement {

    private String sequenceName;

    public CreateSequenceStatement(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    @Override
    public boolean skipOnUnsupported() {
        return true;
    }

    public String getSequenceName() {
        return sequenceName;
    }
}
