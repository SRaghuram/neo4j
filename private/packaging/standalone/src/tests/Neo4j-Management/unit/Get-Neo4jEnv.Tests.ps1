#
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {  
  Describe "Get-Neo4jEnv" {

    It "should return null if env var does not exist" {
      Get-Neo4jEnv -Name 'somevariablenamethatdoesnotexist' | Should BeNullOrEmpty
    }

    $envPath = $Env:Path
    It "should return env var if name is lower case" {
      Get-Neo4jEnv -Name 'path' | Should Be $envPath
    }

    It "should return env var if name is uppercase case" {
      Get-Neo4jEnv -Name 'PATH' | Should Be $envPath
    }
  }
}
