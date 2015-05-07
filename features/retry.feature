Feature: Retry

    Background:
        Given a retry connection "conn" limited to 1 retry

    Scenario: MySQL has Gone Away
        Given MySQL has Gone Away on "conn"
          And I exec "SET SESSION wait_timeout = 1" on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" retry limit should be 0

    Scenario: Lock wait timeout exceeded
        Given a retry connection "once" limited to 1 retry
          And I exec "SET SESSION innodb_lock_wait_timeout = 1" on "@master"
          And I exec "SET SESSION innodb_lock_wait_timeout = 1" on "once"
          And there is a table "test_lock" on "@master"
          And I exec "CREATE TABLE $tableName (id INT PRIMARY KEY) Engine=InnoDb" on "@master"
          And I exec "INSERT INTO test_lock (id) VALUES (1)" on "@master"
          And I start a transaction on "@master"
          And I exec "UPDATE test_lock SET id = 2" on "@master"
         When I exec "UPDATE test_lock SET id = 3" on "once"
         Then the last query failed on "conn"
          And the last error code should be 1205 on "once"
          And "once" retry limit should be 0

    Scenario: Deadlock found when trying to get lock
         When I create a deadlock on "conn" with "@master"
         Then the last query succeeded on "conn"
          And the last error code should be 1213 on "conn"
          And "conn" retry limit should be 0

    # Scenario: ACCESS_DENIED_ERROR restart on another slave
    #     Given SELECT is denied on "conn"
    #      When I query "SELECT 1" on "conn"
    #      Then "conn" retry limit should be 2

    Scenario: ER_DBACCESS_DENIED_ERROR don't restart
        Given a retry connection "conn" limited to 1 retry with db "wrong_db"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error code should be 1044 on "conn"
          And "conn" retry limit should be 1

    Scenario: ACCESS_DENIED_ERROR don't restart
        Given a retry connection "conn" limited to 1 retry with username "nobody"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error code should be 1045 on "conn"
          And "conn" retry limit should be 1

    Scenario: Don't restart transaction
        Given a transaction is started on "conn"
          And MySQL has Gone Away on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And "conn" retry limit should be 1

    Scenario: Restart after transaction is done
        Given a transaction is started on "conn"
         When I query "SELECT 1" on "conn"
          And I commit the transaction on "conn"
          And MySQL has Gone Away on "conn"
          And I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" retry limit should be 0
