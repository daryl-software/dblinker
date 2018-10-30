@retry @master-slaves
Feature: Retry Master/Slaves
   Scenario: ACCESS_DENIED_ERROR restart on another slave
        Given a retry master-slaves connection "test1" with 2 slaves limited to 1 retry with username "nobody"
         When I query "SELECT 1" on "test1"
         Then the last query failed on "test1"
          And the last error should be "ACCESS_DENIED" on "test1"
          And "test1" retry limit should be 0
          And "test1" should have 1 slave

    Scenario: ER_BAD_DB_ERROR restart on another slave
        Given a retry master-slaves connection "test2" with 2 slaves limited to 1 retry with db "unknown_db"
         When I query "SELECT 1" on "test2"
         Then the last query failed on "test2"
          And the last error should be "BAD_DB" on "test2"
          And "test2" retry limit should be 0
          And "test2" should have 1 slave

    @skip-pdo-pgsql
    Scenario: database has Gone Away
        Given a retry master-slaves connection "test3" with 2 slaves limited to 1 retry
          And requests are forced on master for "test3"
          And database has Gone Away on "test3"
         When I query "SELECT 1" on "test3"
         Then the last query succeeded on "test3"
          And the last error should be "GONE_AWAY" on "test3"
          And "test3" retry limit should be 0
          And "test3" should have 2 slaves

    Scenario: ACCESS_DENIED_ERROR does not restart on master
        Given a retry master-slaves connection "test4" with 2 slaves limited to 1 retry with username "nobody"
          And requests are forced on master for "test4"
         When I query "SELECT 1" on "test4"
         Then the last query failed on "test4"
          And the last error should be "ACCESS_DENIED" on "test4"
          And "test4" retry limit should be 1
          And "test4" should have 2 slaves

    Scenario: ER_BAD_DB_ERROR does not restart on master
        Given a retry master-slaves connection "conn" with 2 slaves limited to 1 retry with db "unknown_db" and username "root"
          And requests are forced on master for "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "BAD_DB" on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 2 slaves

    @skip-travis @skip-mysqli
    Scenario: Replication is stopped on slave and query restart on another slave
        Given a retry master-slaves connection "conn" with 2 slaves limited to 1 retry
          And slave replication is stopped on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" retry limit should be 1
          And "conn" should have 1 slaves

    @skip-mysqli @skip-travis @skip-pdo-mysql
    Scenario: Connect only once
        Given a retry master-slaves connection "connce" with 2 slaves limited to 1 retry
         When I query "SELECT 1" on "connce"
         Then I query "SELECT 2" on "connce"
         Then I query "SELECT 3" on "connce"
         Then I query "SELECT 4" on "connce"
         Then I query "SELECT 5" on "connce"
         Then I query "SELECT 6" on "connce"
         Then I query "SELECT 7" on "connce"
         Then I query "SELECT 8" on "connce"
         Then I query "SELECT 9" on "connce"
         Then I query "SELECT 10" on "connce"
          And "connce" retry limit should be 1
          And "connce" should have 2 slaves
          And there is 2 connections established on "connce"

