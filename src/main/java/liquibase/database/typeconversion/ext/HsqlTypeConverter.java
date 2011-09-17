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
import liquibase.util.StringUtils;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 *
 * @author Leo Przybylski
 */
public class HsqlTypeConverter extends liquibase.database.typeconversion.core.HsqlTypeConverter {

    public int getPriority() {
        return EXTENSION_PRIORITY;
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
