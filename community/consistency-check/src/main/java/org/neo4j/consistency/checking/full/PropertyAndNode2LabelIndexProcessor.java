/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.util.List;

import org.neo4j.consistency.checking.PropertyRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.full.RecordProcessor;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

public class PropertyAndNode2LabelIndexProcessor implements RecordProcessor<NodeRecord>
{
    private final ConsistencyReporter reporter;
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeIndexCheck;
    private  RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyCheck;

    public PropertyAndNode2LabelIndexProcessor( ConsistencyReporter reporter,
                                        IndexAccessors indexes,
                                        PropertyReader propertyReader )
    {
        this.reporter = reporter;
        this.nodeIndexCheck = new PropertyAndNodeIndexedCheck( indexes, propertyReader);
        this.propertyCheck = new PropertyRecordCheck();        
        PropertyCache.createPropertyCache(propertyReader.getPropertyStore().getHighId());
    }

    @Override
    public void process( NodeRecord nodeRecord )
    {
        reporter.forNode( nodeRecord, nodeIndexCheck );
        PropertyCache.processProperties( nodeRecord, propertyCheck, reporter );
    }

    @Override
    public void close()
    {
        FullCheckNewUtils.saveMessage( "Done with Index" );
    }
}
