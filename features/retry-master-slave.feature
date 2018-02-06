@retry @master-slaves
Feature: Retry Master/Slaves

    Scenario: Get database
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry
        Then I can get the database name on "conn"

    Scenario: ACCESS_DENIED_ERROR restart on another slave
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with username "nobody"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "ACCESS_DENIED" on "conn"
          And "conn" retry limit should be 0
          And "conn" should have 1 slave

    @skip-mysql-replic
    Scenario: ER_BAD_DB_ERROR restart on another slave
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with db "unknown_db"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "DBACCESS_DENIED" on "conn"
          And "conn" retry limit should be 0
          And "conn" should have 1 slave
    @skip-pdo-pgsql
    Scenario: database has Gone Away
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry
          And requests are forced on master for "conn"
          And database has Gone Away on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And the last error should be "GONE_AWAY" on "conn"
          And "conn" retry limit should be 0
          And "conn" should have 2 slaves

    Scenario: ACCESS_DENIED_ERROR does not restart on master
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with username "nobody"
          And requests are forced on master for "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "ACCESS_DENIED" on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 2 slaves

    @skip-mysql-replic
    Scenario: ER_BAD_DB_ERROR does not restart on master
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with db "unknown_db" and username "root"
          And requests are forced on master for "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "BAD_DB" on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 2 slaves

    @skip-travis @skip-mysqli @skip-pdo-pgsql
    Scenario: Replication is stopped on slave and query restart on another slave
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry
          And slave replication is stopped on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 1 slaves

    @skip-mysqli @skip-mysql-replic @skip-travis @skip-pdo-mysql
    Scenario: Connect only once
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry
         When I query "SELECT 1" on "conn"
         Then I query "SELECT 2" on "conn"
         Then I query "SELECT 3" on "conn"
         Then I query "SELECT 4" on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 2 slaves
          And there is 1 connections established on "conn"

