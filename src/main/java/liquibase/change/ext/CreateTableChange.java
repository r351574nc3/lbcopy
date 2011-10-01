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
package liquibase.change.ext;

import liquibase.change.*;
import liquibase.database.Database;
import liquibase.database.structure.type.DataType;
import liquibase.database.typeconversion.TypeConverter;
import liquibase.database.typeconversion.TypeConverterFactory;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.*;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 * Creates a new table.
 * 
 * @author Leo Przybylski
 */
public class CreateTableChange extends liquibase.change.core.CreateTableChange {

    public CreateTableChange() {
        setPriority(EXTENSION_PRIORITY);
    }

    public void addColumn(ColumnConfig column) {
        super.addColumn(column);
    }

    public SqlStatement[] generateStatements(Database database) {
        final SqlStatement[] retval = super.generateStatements(database);
        for (final SqlStatement statement : retval) {
            for (final DataType type : ((CreateTableStatement) statement).getColumnTypes().values()) {
                System.out.println("Found column type: " + type);
            }
        }
        return retval;
    }
}
