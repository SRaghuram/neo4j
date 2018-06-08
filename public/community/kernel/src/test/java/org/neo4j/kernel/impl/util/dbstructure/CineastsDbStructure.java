/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.util.dbstructure;

import org.neo4j.helpers.collection.Visitable;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;

//
// GENERATED FILE. DO NOT EDIT.
//
// This has been generated by:
//
//   org.neo4j.kernel.impl.util.dbstructure.DbStructureTool
//   org.neo4j.kernel.impl.util.dbstructure.CineastsDbStructure [<output source root>] <db-dir>
//
// (using org.neo4j.kernel.impl.util.dbstructure.InvocationTracer)
//

public enum CineastsDbStructure implements Visitable<DbStructureVisitor>
{
    INSTANCE;

    @Override
    public void accept( DbStructureVisitor visitor )
    {
        visitor.visitLabel( 0, "Movie" );
        visitor.visitLabel( 1, "Person" );
        visitor.visitLabel( 2, "User" );
        visitor.visitLabel( 3, "Actor" );
        visitor.visitLabel( 4, "Director" );
        visitor.visitPropertyKey( 0, "startTime" );
        visitor.visitPropertyKey( 1, "__type__" );
        visitor.visitPropertyKey( 2, "password" );
        visitor.visitPropertyKey( 3, "login" );
        visitor.visitPropertyKey( 4, "roles" );
        visitor.visitPropertyKey( 5, "name" );
        visitor.visitPropertyKey( 6, "description" );
        visitor.visitPropertyKey( 7, "id" );
        visitor.visitPropertyKey( 8, "releaseDate" );
        visitor.visitPropertyKey( 9, "title" );
        visitor.visitPropertyKey( 10, "tagline" );
        visitor.visitPropertyKey( 11, "language" );
        visitor.visitPropertyKey( 12, "imageUrl" );
        visitor.visitPropertyKey( 13, "lastModified" );
        visitor.visitPropertyKey( 14, "genre" );
        visitor.visitPropertyKey( 15, "studio" );
        visitor.visitPropertyKey( 17, "imdbId" );
        visitor.visitPropertyKey( 16, "trailer" );
        visitor.visitPropertyKey( 19, "homepage" );
        visitor.visitPropertyKey( 18, "version" );
        visitor.visitPropertyKey( 21, "profileImageUrl" );
        visitor.visitPropertyKey( 20, "runtime" );
        visitor.visitPropertyKey( 23, "birthday" );
        visitor.visitPropertyKey( 22, "biography" );
        visitor.visitPropertyKey( 25, "stars" );
        visitor.visitPropertyKey( 24, "birthplace" );
        visitor.visitPropertyKey( 26, "comment" );
        visitor.visitRelationshipType( 0, "FRIEND" );
        visitor.visitRelationshipType( 1, "DIRECTED" );
        visitor.visitRelationshipType( 2, "ACTS_IN" );
        visitor.visitRelationshipType( 3, "RATED" );
        visitor.visitRelationshipType( 4, "ROOT" );
        visitor.visitIndex( TestIndexDescriptorFactory.forLabel( 0, 9 ), ":Movie(title)", 1.0d, 12462L );
        visitor.visitIndex( TestIndexDescriptorFactory.forLabel( 1, 5 ), ":Person(name)", 1.0d, 49845L );
        visitor.visitIndex( TestIndexDescriptorFactory.forLabel( 3, 5 ), ":Actor(name)", 1.0d, 44689L );
        visitor.visitIndex( TestIndexDescriptorFactory.forLabel( 4, 5 ), ":Director(name)", 1.0d, 6010L );
        visitor.visitIndex( TestIndexDescriptorFactory.uniqueForLabel( 2, 3 ), ":User(login)", 1.0d, 45L );
        visitor.visitUniqueConstraint( ConstraintDescriptorFactory.uniqueForLabel( 2, 3 ),
                "CONSTRAINT ON ( " + "user:User ) ASSERT user.login IS UNIQUE" );
        visitor.visitAllNodesCount( 63042L );
        visitor.visitNodeCount( 0, "Movie", 12862L );
        visitor.visitNodeCount( 1, "Person", 50179L );
        visitor.visitNodeCount( 2, "User", 45L );
        visitor.visitNodeCount( 3, "Actor", 44943L );
        visitor.visitNodeCount( 4, "Director", 6037L );
        visitor.visitRelCount( -1, -1, -1, "MATCH ()-[]->() RETURN count(*)", 106651L );
        visitor.visitRelCount( 0, -1, -1, "MATCH (:Movie)-[]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, -1, 0, "MATCH ()-[]->(:Movie) RETURN count(*)", 106645L );
        visitor.visitRelCount( 1, -1, -1, "MATCH (:Person)-[]->() RETURN count(*)", 106651L );
        visitor.visitRelCount( -1, -1, 1, "MATCH ()-[]->(:Person) RETURN count(*)", 6L );
        visitor.visitRelCount( 2, -1, -1, "MATCH (:User)-[]->() RETURN count(*)", 36L );
        visitor.visitRelCount( -1, -1, 2, "MATCH ()-[]->(:User) RETURN count(*)", 6L );
        visitor.visitRelCount( 3, -1, -1, "MATCH (:Actor)-[]->() RETURN count(*)", 97151L );
        visitor.visitRelCount( -1, -1, 3, "MATCH ()-[]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, -1, -1, "MATCH (:Director)-[]->() RETURN count(*)", 16268L );
        visitor.visitRelCount( -1, -1, 4, "MATCH ()-[]->(:Director) RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 0, -1, "MATCH ()-[:FRIEND]->() RETURN count(*)", 6L );
        visitor.visitRelCount( 0, 0, -1, "MATCH (:Movie)-[:FRIEND]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 0, 0, "MATCH ()-[:FRIEND]->(:Movie) RETURN count(*)", 0L );
        visitor.visitRelCount( 1, 0, -1, "MATCH (:Person)-[:FRIEND]->() RETURN count(*)", 6L );
        visitor.visitRelCount( -1, 0, 1, "MATCH ()-[:FRIEND]->(:Person) RETURN count(*)", 6L );
        visitor.visitRelCount( 2, 0, -1, "MATCH (:User)-[:FRIEND]->() RETURN count(*)", 6L );
        visitor.visitRelCount( -1, 0, 2, "MATCH ()-[:FRIEND]->(:User) RETURN count(*)", 6L );
        visitor.visitRelCount( 3, 0, -1, "MATCH (:Actor)-[:FRIEND]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 0, 3, "MATCH ()-[:FRIEND]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, 0, -1, "MATCH (:Director)-[:FRIEND]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 0, 4, "MATCH ()-[:FRIEND]->(:Director) RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 1, -1, "MATCH ()-[:DIRECTED]->() RETURN count(*)", 11915L );
        visitor.visitRelCount( 0, 1, -1, "MATCH (:Movie)-[:DIRECTED]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 1, 0, "MATCH ()-[:DIRECTED]->(:Movie) RETURN count(*)", 11915L );
        visitor.visitRelCount( 1, 1, -1, "MATCH (:Person)-[:DIRECTED]->() RETURN count(*)", 11915L );
        visitor.visitRelCount( -1, 1, 1, "MATCH ()-[:DIRECTED]->(:Person) RETURN count(*)", 0L );
        visitor.visitRelCount( 2, 1, -1, "MATCH (:User)-[:DIRECTED]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 1, 2, "MATCH ()-[:DIRECTED]->(:User) RETURN count(*)", 0L );
        visitor.visitRelCount( 3, 1, -1, "MATCH (:Actor)-[:DIRECTED]->() RETURN count(*)", 2451L );
        visitor.visitRelCount( -1, 1, 3, "MATCH ()-[:DIRECTED]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, 1, -1, "MATCH (:Director)-[:DIRECTED]->() RETURN count(*)", 11915L );
        visitor.visitRelCount( -1, 1, 4, "MATCH ()-[:DIRECTED]->(:Director) RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 2, -1, "MATCH ()-[:ACTS_IN]->() RETURN count(*)", 94700L );
        visitor.visitRelCount( 0, 2, -1, "MATCH (:Movie)-[:ACTS_IN]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 2, 0, "MATCH ()-[:ACTS_IN]->(:Movie) RETURN count(*)", 94700L );
        visitor.visitRelCount( 1, 2, -1, "MATCH (:Person)-[:ACTS_IN]->() RETURN count(*)", 94700L );
        visitor.visitRelCount( -1, 2, 1, "MATCH ()-[:ACTS_IN]->(:Person) RETURN count(*)", 0L );
        visitor.visitRelCount( 2, 2, -1, "MATCH (:User)-[:ACTS_IN]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 2, 2, "MATCH ()-[:ACTS_IN]->(:User) RETURN count(*)", 0L );
        visitor.visitRelCount( 3, 2, -1, "MATCH (:Actor)-[:ACTS_IN]->() RETURN count(*)", 94700L );
        visitor.visitRelCount( -1, 2, 3, "MATCH ()-[:ACTS_IN]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, 2, -1, "MATCH (:Director)-[:ACTS_IN]->() RETURN count(*)", 4353L );
        visitor.visitRelCount( -1, 2, 4, "MATCH ()-[:ACTS_IN]->(:Director) RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 3, -1, "MATCH ()-[:RATED]->() RETURN count(*)", 30L );
        visitor.visitRelCount( 0, 3, -1, "MATCH (:Movie)-[:RATED]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 3, 0, "MATCH ()-[:RATED]->(:Movie) RETURN count(*)", 30L );
        visitor.visitRelCount( 1, 3, -1, "MATCH (:Person)-[:RATED]->() RETURN count(*)", 30L );
        visitor.visitRelCount( -1, 3, 1, "MATCH ()-[:RATED]->(:Person) RETURN count(*)", 0L );
        visitor.visitRelCount( 2, 3, -1, "MATCH (:User)-[:RATED]->() RETURN count(*)", 30L );
        visitor.visitRelCount( -1, 3, 2, "MATCH ()-[:RATED]->(:User) RETURN count(*)", 0L );
        visitor.visitRelCount( 3, 3, -1, "MATCH (:Actor)-[:RATED]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 3, 3, "MATCH ()-[:RATED]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, 3, -1, "MATCH (:Director)-[:RATED]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 3, 4, "MATCH ()-[:RATED]->(:Director) RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, -1, "MATCH ()-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( 0, 4, -1, "MATCH (:Movie)-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, 0, "MATCH ()-[:ROOT]->(:Movie) RETURN count(*)", 0L );
        visitor.visitRelCount( 1, 4, -1, "MATCH (:Person)-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, 1, "MATCH ()-[:ROOT]->(:Person) RETURN count(*)", 0L );
        visitor.visitRelCount( 2, 4, -1, "MATCH (:User)-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, 2, "MATCH ()-[:ROOT]->(:User) RETURN count(*)", 0L );
        visitor.visitRelCount( 3, 4, -1, "MATCH (:Actor)-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, 3, "MATCH ()-[:ROOT]->(:Actor) RETURN count(*)", 0L );
        visitor.visitRelCount( 4, 4, -1, "MATCH (:Director)-[:ROOT]->() RETURN count(*)", 0L );
        visitor.visitRelCount( -1, 4, 4, "MATCH ()-[:ROOT]->(:Director) RETURN count(*)", 0L );
   }
}

/* END OF GENERATED CONTENT */
