package liquibase.database.typeconversion.ext;

import liquibase.database.structure.Column;
import liquibase.database.structure.type.BlobType;
import liquibase.database.structure.type.ClobType;
import liquibase.database.structure.type.DateTimeType;
import liquibase.database.structure.type.NVarcharType;
import liquibase.database.structure.type.NumberType;
import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.database.core.HsqlDatabase;

public class HsqlTypeConverter extends liquibase.database.typeconversion.core.HsqlTypeConverter {

    public int getPriority() {
        return 1000;
    }

    public String convertToDatabaseTypeString(Column referenceColumn, Database database) {
        if (referenceColumn.getTypeName().toLowerCase().indexOf("text") > -1) {
            return getClobType().getDataTypeName();
        }
        else if (referenceColumn.getTypeName().toLowerCase().indexOf("varchar") > -1) {
            return getVarcharType().getDataTypeName();
        }

        return super.convertToDatabaseTypeString(referenceColumn, database);
    }
}
