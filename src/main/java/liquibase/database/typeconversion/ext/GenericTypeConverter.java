package liquibase.database.typeconversion.ext;

import liquibase.database.structure.Column;
import liquibase.database.structure.type.*;
import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.database.core.HsqlDatabase;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.util.StringUtils;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;


public class GenericTypeConverter extends liquibase.database.typeconversion.core.AbstractTypeConverter {

    public int getPriority() {
        return 1500;
    }
    
    protected static final List<Integer> oneParam = Arrays.asList(
        Types.CHAR,
        -15, // Types.NCHAR in java 1.6,
        Types.VARCHAR,
        -9, //Types.NVARCHAR in java 1.6,
        Types.VARBINARY,
        Types.DOUBLE,
        Types.FLOAT
        );
        
    protected static final List<Integer> twoParams = Arrays.asList(
        Types.DECIMAL,
        Types.NUMERIC,
        Types.REAL
        );
    
    
    @Override
    public NumberType getNumberType() {
        return new NumberType("NUMERIC");
    }
    
    @Override
    public boolean supports(final Database database) {
        return true;
    }

    public String convertToDatabaseTypeString(Column referenceColumn, Database database) {
        // System.out.println("Converting " + referenceColumn.getTypeName());
        
        final StringBuilder retval = new StringBuilder();
        try {
            retval.append(getSqlTypeName(referenceColumn.getDataType()));
        }
        catch (Exception e) {
            retval.append(referenceColumn.getTypeName());
        }

        
        final boolean hasOneParam  = oneParam.contains(referenceColumn.getDataType());
        final boolean hasTwoParams = twoParams.contains(referenceColumn.getDataType());
        

        if (hasOneParam || hasTwoParams) {
            retval.append("(").append(referenceColumn.getColumnSize());
            if (hasTwoParams) {
                retval.append(",").append(referenceColumn.getDecimalDigits());
            }
            retval.append(")");
        }

        
        return retval.toString();
    }

    
    protected String getSqlTypeName(final int type) throws Exception {
        for (final Field field : Types.class.getFields()) {
            final int sql_type = field.getInt(null);
            if (type == sql_type) {
                return "java.sql.Types." + field.getName();
            }
        }
        return null;
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
            if (columnTypeString.indexOf("DECIMAL") < -1) {
                System.out.println("Looking up " + columnTypeString);
            }
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

    protected DataType getDataType(String columnTypeString, Boolean autoIncrement, String dataTypeName, String precision, String additionalInformation) {
        // Translate type to database-specific type, if possible
        DataType returnTypeName = null;
        if (dataTypeName.equalsIgnoreCase("BIGINT")) {
            returnTypeName = getBigIntType();
        } else if (dataTypeName.equalsIgnoreCase("NUMBER") 
                   || dataTypeName.equalsIgnoreCase("DECIMAL")
                   || dataTypeName.equalsIgnoreCase("NUMERIC")) {
            returnTypeName = getNumberType();
        } else if (dataTypeName.equalsIgnoreCase("BLOB")) {
            returnTypeName = getBlobType();
        } else if (dataTypeName.equalsIgnoreCase("BOOLEAN")) {
            returnTypeName = getBooleanType();
        } else if (dataTypeName.equalsIgnoreCase("CHAR")) {
            returnTypeName = getCharType();
        } else if (dataTypeName.equalsIgnoreCase("CLOB")) {
            returnTypeName = getClobType();
        } else if (dataTypeName.equalsIgnoreCase("CURRENCY")) {
            returnTypeName = getCurrencyType();
        } else if (dataTypeName.equalsIgnoreCase("DATE") || dataTypeName.equalsIgnoreCase(getDateType().getDataTypeName())) {
            returnTypeName = getDateType();
        } else if (dataTypeName.equalsIgnoreCase("DATETIME") || dataTypeName.equalsIgnoreCase(getDateTimeType().getDataTypeName())) {
            returnTypeName = getDateTimeType();
        } else if (dataTypeName.equalsIgnoreCase("DOUBLE")) {
            returnTypeName = getDoubleType();
        } else if (dataTypeName.equalsIgnoreCase("FLOAT")) {
            returnTypeName = getFloatType();
        } else if (dataTypeName.equalsIgnoreCase("INT")) {
            returnTypeName = getIntType();
        } else if (dataTypeName.equalsIgnoreCase("INTEGER")) {
            returnTypeName = getIntType();
        } else if (dataTypeName.equalsIgnoreCase("LONGBLOB")) {
            returnTypeName = getLongBlobType();
        } else if (dataTypeName.equalsIgnoreCase("LONGVARBINARY")) {
            returnTypeName = getBlobType();
        } else if (dataTypeName.equalsIgnoreCase("LONGVARCHAR")) {
            returnTypeName = getClobType();
        } else if (dataTypeName.equalsIgnoreCase("SMALLINT")) {
            returnTypeName = getSmallIntType();
        } else if (dataTypeName.equalsIgnoreCase("TEXT")) {
            returnTypeName = getClobType();
        } else if (dataTypeName.equalsIgnoreCase("TIME") || dataTypeName.equalsIgnoreCase(getTimeType().getDataTypeName())) {
            returnTypeName = getTimeType();
        } else if (dataTypeName.toUpperCase().contains("TIMESTAMP")) {
            returnTypeName = getDateTimeType();
        } else if (dataTypeName.equalsIgnoreCase("TINYINT")) {
            returnTypeName = getTinyIntType();
        } else if (dataTypeName.equalsIgnoreCase("UUID")) {
            returnTypeName = getUUIDType();
        } else if (dataTypeName.equalsIgnoreCase("VARCHAR")) {
            returnTypeName = getVarcharType();
        } else if (dataTypeName.equalsIgnoreCase("NVARCHAR")) {
            returnTypeName = getNVarcharType();
        } else {
            return new CustomType(columnTypeString,0,2);
        }

        if (returnTypeName == null) {
            throw new UnexpectedLiquibaseException("Could not determine " + dataTypeName + " for " + this.getClass().getName());
        }
        addPrecisionToType(precision, returnTypeName);
        returnTypeName.setAdditionalInformation(additionalInformation);

         return returnTypeName;
    }

}
