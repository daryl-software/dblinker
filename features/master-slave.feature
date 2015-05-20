Feature: Master / Slaves

    Background:
        Given a master/slaves connection "conn" with 3 slaves

    Scenario: Read request on slave
         When I query "SELECT 1" on "conn"
         Then "conn" is on slave

    Scenario: "Write" request on master
         When I exec "SET @var = 1" on "conn"
         Then "conn" is on master

    Scenario: Keep slave after "Write" request
         When I exec "SET @var = 1" on "conn"
          And I query "SELECT 1" on "conn"
         Then "conn" is on master

    Scenario: Connect on master when there is no slaves
        Given a master/slaves connection "connMaster" with no slaves
         When I query "SELECT 1" on "connMaster"
         Then "connMaster" is on master

    Scenario: Force request on master
         When I force requests on master for "conn"
         Then "conn" is on master

    Scenario: Force request back on slave
        Given requests are forced on master for "conn"
         When I force requests on slave for "conn"
         When I query "SELECT 1" on "conn"

    Scenario: Connect on master for transaction
        Given a transaction is started on "conn"
         When I query "SELECT 1" on "conn"
         Then "conn" is on master
