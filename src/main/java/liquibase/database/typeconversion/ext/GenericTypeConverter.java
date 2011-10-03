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
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;


/**
 * Converts to and from generic SQL Types (types located in {@link Types}). Conversion happens at 
 * update and generate phases. All databases are supported because the idea is a generic type
 * that is a median type and can be used to convert between other types. For example, when exporting
 * from an Oracle database, can be converted to a MySQL database quickly.
 * 
 * @author Leo Przybylski
 */
public class GenericTypeConverter extends liquibase.database.typeconversion.core.AbstractTypeConverter {

    public int getPriority() {
        return EXTENSION_PRIORITY;
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
    public Object convertDatabaseValueToObject(Object value, int databaseDataType, int firstParameter, int secondParameter, Database database) throws ParseException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return convertToCorrectObjectType(((String) value).trim().replaceFirst("^'", "").replaceFirst("'$", ""), databaseDataType, firstParameter, secondParameter, database);
        } else {
            return value;
        }
    }

    /**
     * Supports all databases, so this always returns true
     *
     * @param database (doesn't matter)
     * @return true always
     */
    @Override
    public boolean supports(final Database database) {
        return !(database instanceof liquibase.database.core.MySQLDatabase);
    }

    @Override
    public String convertToDatabaseTypeString(Column referenceColumn, Database database) {        
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

    /**
     * Convert the type value gotten from the metadata which is a value from {@link Types} to a {@link String} value
     * that can be used in an SQL statement. Example output:
     * <ul>
     *   <li>java.sql.Types.DECIMAL(25,0)</li>
     *   <li>java.sql.Types.BIGINT</li>
     *   <li>java.sql.Types.VARCHAR(255)</li>
     * </ul>
     *
     * @param type int value found in {@linK Types}
     * @return String value including package of the type name.
     */
    protected String getSqlTypeName(final int type) throws Exception {
        for (final Field field : Types.class.getFields()) {
            final int sql_type = field.getInt(null);
            if (type == sql_type) {
                return "java.sql.Types." + field.getName();
            }
        }
        return null;
    }

    @Override
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

	@Override
	public BooleanType getBooleanType() {
		return new BooleanType.NumericBooleanType(getNumberType().getDataTypeName());
	}
}
