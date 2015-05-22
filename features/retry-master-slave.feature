@retry @master-slaves
Feature: Retry Master/Slaves

    Background:
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry

    Scenario: ACCESS_DENIED_ERROR restart on another slave
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with username "nobody"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error code should be 1045 on "conn"
          And "conn" retry limit should be 0

    Scenario: ACCESS_DENIED_ERROR does not restart on master
        Given a retry master/slaves connection "conn" with 2 slaves limited to 1 retry with username "nobody"
          And requests are forced on master for "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error code should be 1045 on "conn"
          And "conn" retry limit should be 1
