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
 * @Deprecated 
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
    public BooleanType getBooleanType() {
	return new BooleanType.NumericBooleanType(getNumberType().getDataTypeName());
    }
}
