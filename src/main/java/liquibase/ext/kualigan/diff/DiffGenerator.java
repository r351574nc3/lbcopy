// Copyright 2014 Leo Przybylski. All rights reserved.
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
package liquibase.ext.kualigan.diff;


import liquibase.database.Database;
import liquibase.diff.DiffResult;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.StringDiff;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.servicelocator.PrioritizedService;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.EmptyDatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Index;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import liquibase.structure.core.View;
import liquibase.util.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

import static liquibase.ext.Constants.EXTENSION_PRIORITY;

/**
 * Extension of {@link liquibase.diff.DiffGenerator}. Add support for JDBC types and schema-agnostic handling
 *
 * @author Leo Przybylski
 */
public class DiffGenerator implements liquibase.diff.DiffGenerator {
    @Override
    public int getPriority() {
        return EXTENSION_PRIORITY;
    }

    @Override
    public boolean supports(final Database referenceDatabase, final Database comparisonDatabase) {
        return true;
    }

    @Override
    public DiffResult compare(final DatabaseSnapshot referenceSnapshot, 
                              DatabaseSnapshot comparisonSnapshot, 
                              final CompareControl compareControl) throws DatabaseException {
                

        if (comparisonSnapshot == null) {
            try {
                comparisonSnapshot = new EmptyDatabaseSnapshot(referenceSnapshot.getDatabase()); //, compareControl.toSnapshotControl(CompareControl.DatabaseRole.REFERENCE));
            } catch (InvalidExampleException e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }

        final DiffResult diffResult = new DiffResult(referenceSnapshot, comparisonSnapshot, compareControl);
        checkVersionInfo(referenceSnapshot, comparisonSnapshot, diffResult);

        final Set<Class<? extends DatabaseObject>> typesToCompare = compareControl.getComparedTypes();
        typesToCompare.retainAll(referenceSnapshot.getSnapshotControl().getTypesToInclude());
        typesToCompare.retainAll(comparisonSnapshot.getSnapshotControl().getTypesToInclude());

        for (final Class<? extends DatabaseObject> typeToCompare : typesToCompare) {
            compareObjectType(typeToCompare, referenceSnapshot, comparisonSnapshot, diffResult);
        }

	//        // Hack:  Sometimes Indexes or Unique Constraints with multiple columns get added twice (1 for each column),
	//        // so we're combining them back to a single Index or Unique Constraint here.
	//        removeDuplicateIndexes( diffResult.getMissingIndexes() );
	//        removeDuplicateIndexes( diffResult.getUnexpectedIndexes() );
	//        removeDuplicateUniqueConstraints( diffResult.getMissingUniqueConstraints() );
	//        removeDuplicateUniqueConstraints( diffResult.getUnexpectedUniqueConstraints() );

        return diffResult;
    }

    protected <T extends DatabaseObject> void compareObjectType(Class<T> type, DatabaseSnapshot referenceSnapshot, DatabaseSnapshot comparisonSnapshot, DiffResult diffResult) {

        CompareControl.SchemaComparison[] schemaComparisons = diffResult.getCompareControl().getSchemaComparisons();
        if (schemaComparisons != null) {
            for (CompareControl.SchemaComparison schemaComparison : schemaComparisons) {
                for (T referenceObject : referenceSnapshot.get(type)) {
                    //                if (referenceObject instanceof Table && referenceSnapshot.getDatabase().isLiquibaseTable(referenceSchema, referenceObject.getName())) {
                    //                    continue;
                    //                }
                    T comparisonObject = comparisonSnapshot.get(referenceObject);
                    if (comparisonObject == null) {
                        diffResult.addMissingObject(referenceObject);
                    } else {
                        ObjectDifferences differences = DatabaseObjectComparatorFactory.getInstance().findDifferences(referenceObject, comparisonObject, comparisonSnapshot.getDatabase(), diffResult.getCompareControl());
                        if (differences.hasDifferences()) {
                            diffResult.addChangedObject(referenceObject, differences);
                        }
                    }
                }
                //
                for (T comparisonObject : comparisonSnapshot.get(type)) {
                    //                if (targetObject instanceof Table && comparisonSnapshot.getDatabase().isLiquibaseTable(comparisonSchema, targetObject.getName())) {
                    //                    continue;
                    //                }
                    if (referenceSnapshot.get(comparisonObject) == null) {
                        diffResult.addUnexpectedObject(comparisonObject);
                    }
                    //            }
                }
            }

            //todo: add logic for when container is missing or unexpected also
        }
    }

    @Override
    protected void checkVersionInfo(final DatabaseSnapshot referenceSnapshot, 
				    final DatabaseSnapshot comparisonSnapshot, 
				    final DiffResult diffResult) throws DatabaseException {

        if (comparisonSnapshot != null && comparisonSnapshot.getDatabase() != null) {
            diffResult.setProductNameDiff(new StringDiff(referenceSnapshot.getDatabase().getDatabaseProductName(), comparisonSnapshot.getDatabase().getDatabaseProductName()));
            diffResult.setProductVersionDiff(new StringDiff(referenceSnapshot.getDatabase().getDatabaseProductVersion(), comparisonSnapshot.getDatabase().getDatabaseProductVersion()));
        }

    }
}

