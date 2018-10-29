@master-slaves
Feature: Master / Slaves

    Background:
      Given a master-slaves connection "conn" with 3 slaves
      And slave user has only SELECT permission on "conn"

    Scenario: Insert a row on master
    When I query "INSERT INTO users (name, email) VALUES ('test', ?)" with param "bob@test.com" on "conn"
    Then the last query succeeded on "conn"
    And "conn" is on master

    Scenario: Read prepared statement on slave
        When I query "SELECT * FROM users WHERE email = ?" with param 'max@yopmail.com' on "conn"
        Then the last query succeeded on "conn"
         And "conn" is on slave

    Scenario: Read request on slave
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on slave

    Scenario: "Write" request on master
         When I exec "SET @var = 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on master

    Scenario: Keep slave after "Write" request
         When I exec "SET @var = 1" on "conn"
          And I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on slave

    Scenario: Connect on master when there is no slaves
        Given a master-slaves connection "connMaster" with 0 slaves
         When I query "SELECT 1" on "connMaster"
         Then the last query succeeded on "connMaster"
          And "connMaster" is on master

    Scenario: Connect on master for transaction
        Given a transaction is started on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on master

    Scenario: Disable cache
        Given the cache is disable on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on slave

    Scenario: Test executeUpdate
        Given the cache is disable on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" is on slave
