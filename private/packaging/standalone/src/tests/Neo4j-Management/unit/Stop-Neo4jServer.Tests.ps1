#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.",".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
.$common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Stop-Neo4jServer" {

    # Setup mocking environment
    #  Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' }
    Mock Set-Neo4jEnv {}
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }
    # Mock Neo4j environment
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Confirm-JavaVersion { $true }
    Mock Start-Process { throw "Should not call Start-Process mock" }
    Mock Invoke-ExternalCommand { throw "Should not call Invoke-ExternalCommand mock" }

    Context "Missing service name in configuration files" {
      Mock Invoke-ExternalCommand {}

      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "throws error for missing service name in configuration file" {
        { Stop-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Stop service failed" {
      Mock Get-Service { return 'service' }
      Mock Invoke-ExternalCommand { throw "Called Stop-Service incorrectly" }
      Mock Invoke-ExternalCommand -Verifiable { @{ exitCode = 1; capturedOutput = 'failed to stop' } } -ParameterFilter { $Command -like '*prunsrv*.exe' }

      $serverObject = global:New-MockNeo4jInstall

      $result = Stop-Neo4jServer -Neo4jServer $serverObject
      It "result is 1" {
        $result | Should Be 1
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }

    Context "Stop service succesfully" {
      Mock Get-Service { return 'service' }
      Mock Invoke-ExternalCommand { throw "Called Stop-Service incorrectly" }
      Mock Invoke-ExternalCommand -Verifiable { @{ exitCode = 0 } } -ParameterFilter { $Command -like '*prunsrv*.exe' }

      $serverObject = global:New-MockNeo4jInstall

      $result = Stop-Neo4jServer -Neo4jServer $serverObject
      It "result is 0" {
        $result | Should Be 0
      }

      It "calls verified mocks" {
        Assert-VerifiableMocks
      }
    }
  }
}
