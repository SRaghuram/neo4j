#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
#

Feature: IndexAcceptance

  Scenario: Handling numerical literal on the left when using an index
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Product(unitsInStock)
      """
    And having executed:
      """
      CREATE (:Product {unitsInStock: 8})
      CREATE (:Product {unitsInStock: 12})
      """
    When executing query:
      """
      MATCH (p:Product)
      WHERE 10 < p.unitsInStock
      RETURN p
      """
    Then the result should be:
      | p                             |
      | (:Product {unitsInStock: 12}) |
    And no side effects

  Scenario: Handling numerical literal on the right when using an index
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Product(unitsInStock)
      """
    And having executed:
      """
      CREATE (:Product {unitsInStock: 8})
      CREATE (:Product {unitsInStock: 12})
      """
    When executing query:
      """
      MATCH (p:Product)
      WHERE p.unitsInStock > 10
      RETURN p
      """
    Then the result should be:
      | p                             |
      | (:Product {unitsInStock: 12}) |
    And no side effects

  Scenario: Works fine with index
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Person(name)
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse'})
      RETURN person.name
      """
    Then the result should be:
      | person.name |
      | 'Lasse'     |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |

  Scenario: Works with indexed and unindexed property
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Person(name)
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse', id: 42})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Works with two indexed properties
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Person(name)
      """
    And having executed:
      """
      CREATE INDEX ON :Person(id)
      """
    When executing query:
      """
      MERGE (person:Person {name: 'Lasse', id: 42})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 2 |

  Scenario: Should be able to merge using property from match with index
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :City(name)
      """
    And having executed:
      """
      CREATE (:Person {name: 'A', bornIn: 'New York'})
      CREATE (:Person {name: 'B', bornIn: 'Ohio'})
      CREATE (:Person {name: 'C', bornIn: 'New Jersey'})
      CREATE (:Person {name: 'D', bornIn: 'New York'})
      CREATE (:Person {name: 'E', bornIn: 'Ohio'})
      CREATE (:Person {name: 'F', bornIn: 'New Jersey'})
      """
    When executing query:
      """
      MATCH (person:Person)
      MERGE (city:City {name: person.bornIn})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +labels     | 3 |
      | +properties | 3 |

  Scenario: Merge with an index must properly handle multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :L(prop)
      """
    And having executed:
      """
      CREATE (:L:A {prop: 42})
      """
    When executing query:
      """
      MERGE (test:L:B {prop: 42})
      RETURN labels(test) AS labels
      """
    Then the result should be:
      | labels     |
      | ['L', 'B'] |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 2 |
      | +properties | 1 |

  Scenario: STARTS WITH should handle null prefix
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Person(name)
      """
    And having executed:
      """
      CREATE (:Person {name: 'Jack'})
      CREATE (:Person {name: 'Jill'})
      """
    When executing query:
      """
      MATCH (p:Person)
      WHERE p.name STARTS WITH null
      RETURN p
      """
    Then the result should be:
      | p                             |
    And no side effects

  Scenario: Index seek should handle null value
    Given an empty graph
    And having executed:
      """
      CREATE INDEX ON :Person(name)
      """
    And having executed:
      """
      CREATE (:Person {name: 'Jack'})
      CREATE (:Person {name: 'Jill'})
      """
    When executing query:
      """
      MATCH (p:Person)
      WHERE p.name = null
      RETURN p
      """
    Then the result should be:
      | p                             |
    And no side effects

