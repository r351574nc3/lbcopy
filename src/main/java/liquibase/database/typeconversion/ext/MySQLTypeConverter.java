package liquibase.database.typeconversion.ext;

import liquibase.database.structure.type.BlobType;
import liquibase.database.structure.type.BooleanType;
import liquibase.database.structure.type.ClobType;
import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.structure.type.DataType;
import liquibase.database.structure.type.DateTimeType;

public class MySQLTypeConverter extends liquibase.database.typeconversion.core.MySQLTypeConverter {

    public int getPriority() {
        return 10;
    }

}
