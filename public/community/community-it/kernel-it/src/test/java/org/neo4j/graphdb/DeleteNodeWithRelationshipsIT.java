/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;

import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ImpermanentDbmsExtension
class DeleteNodeWithRelationshipsIT
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldGiveHelpfulExceptionWhenDeletingNodeWithRelationships()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "MAYOR_OF" ) );
            tx.commit();
        }

        // And given a transaction deleting just the node
        Transaction tx = db.beginTx();
        tx.getNodeById( node.getId() ).delete();

        ConstraintViolationException ex = assertThrows( ConstraintViolationException.class, tx::commit );
        assertEquals( "Cannot delete node<" + node.getId() + ">, because it still has relationships. " +
                "To delete this node, you must first delete its relationships.", ex.getMessage() );
    }

}
