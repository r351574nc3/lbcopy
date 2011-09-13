package liquibase.database.typeconversion.ext;

import liquibase.database.structure.Column;
import liquibase.database.structure.type.*;
import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.database.core.HsqlDatabase;
import liquibase.util.StringUtils;


public class HsqlTypeConverter extends liquibase.database.typeconversion.core.HsqlTypeConverter {

    public int getPriority() {
        return 1000;
    }

    @Override
    public NumberType getNumberType() {
        return new NumberType("NUMERIC");
    }

    public String convertToDatabaseTypeString(Column referenceColumn, Database database) {
        if (referenceColumn.getTypeName().toLowerCase().indexOf("text") > -1) {
            return getClobType().getDataTypeName();
        }
        else if (referenceColumn.getTypeName().toLowerCase().indexOf("varchar") > -1) {
            final VarcharType type = getVarcharType();
            type.setFirstParameter("" + referenceColumn.getColumnSize());
            return type.toString();
        }
        else if (referenceColumn.getTypeName().toLowerCase().indexOf("num") > -1) {
            final NumberType type = new NumberType("NUMERIC");
            type.setFirstParameter("" + referenceColumn.getColumnSize());
            return type.toString();
        }

        return super.convertToDatabaseTypeString(referenceColumn, database);
    }


    /**
     * Returns the database-specific datatype for the given column configuration.
     * This method will convert some generic column types (e.g. boolean, currency) to the correct type
     * for the current database.
     */
    public DataType getDataType(String columnTypeString, Boolean autoIncrement) {
        // Parse out data type and precision
        // Example cases: "CLOB", "java.sql.Types.CLOB", "CLOB(10000)", "java.sql.Types.CLOB(10000)
        String dataTypeName = null;
        String precision = null;
        String additionalInformation = null;
        if (columnTypeString.startsWith("java.sql.Types") && columnTypeString.contains("(")) {
            precision = columnTypeString.substring(columnTypeString.indexOf("(") + 1, columnTypeString.indexOf(")"));
            dataTypeName = columnTypeString.substring(columnTypeString.lastIndexOf(".") + 1, columnTypeString.indexOf("("));
        } else if (columnTypeString.startsWith("java.sql.Types")) {
            dataTypeName = columnTypeString.substring(columnTypeString.lastIndexOf(".") + 1);
        } else if (columnTypeString.contains("(")) {
            precision = columnTypeString.substring(columnTypeString.indexOf("(") + 1, columnTypeString.indexOf(")"));
            dataTypeName = columnTypeString.substring(0, columnTypeString.indexOf("("));
        } else {
            dataTypeName = columnTypeString;
        }
        if (columnTypeString.contains(")")) {
            additionalInformation = StringUtils.trimToNull(columnTypeString.replaceFirst(".*\\)", ""));
        }

        return getDataType(columnTypeString, autoIncrement, dataTypeName, precision, additionalInformation);
    }

}
