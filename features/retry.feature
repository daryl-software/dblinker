@retry
Feature: Retry

    Background:
        Given a retry connection "conn" limited to 1 retry
    @skip-pdo-pgsql
    Scenario: database has Gone Away
        Given database has Gone Away on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And the last error should be "GONE_AWAY" on "conn"
          And "conn" retry limit should be 0
    @skip-pdo-pgsql
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
          And the last error should be "LOCK_WAIT_TIMEOUT" on "once"
          And "once" retry limit should be 0
    @skip-travis @skip-mysqli @skip-pdo-pgsql
    Scenario: Deadlock found when trying to get lock
         When I create a deadlock on "conn" with "@master"
         Then the last query succeeded on "conn"
          And the last error should be "DEADLOCK" on "conn"
          And "conn" retry limit should be 0
    @skip-travis-pdo-pgsql
    Scenario: ER_DBACCESS_DENIED_ERROR don't restart
        Given a retry connection "conn" limited to 1 retry with db "forbidden_db"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "DBACCESS_DENIED" on "conn"
          And "conn" retry limit should be 1

    Scenario: ACCESS_DENIED_ERROR don't restart
        Given a retry connection "conn" limited to 1 retry with username "nobody"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "ACCESS_DENIED" on "conn"
          And "conn" retry limit should be 1

    Scenario: ER_BAD_DB_ERROR don't restart
        Given a retry connection "conn" limited to 1 retry with db "unknown_db" and username "root"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And the last error should be "BAD_DB" on "conn"
          And "conn" retry limit should be 1
    @skip-pdo-pgsql
    Scenario: Don't restart transaction
        Given a transaction is started on "conn"
          And database has Gone Away on "conn"
         When I query "SELECT 1" on "conn"
         Then the last query failed on "conn"
          And "conn" retry limit should be 1
    @skip-pdo-pgsql
    Scenario: Restart after transaction is done
        Given a transaction is started on "conn"
         When I query "SELECT 1" on "conn"
          And I commit the transaction on "conn"
          And database has Gone Away on "conn"
          And I query "SELECT 1" on "conn"
         Then the last query succeeded on "conn"
          And "conn" retry limit should be 0
    @skip-pdo-pgsql
    Scenario: Too many connections
        Given the server accept 1 more connections
          And a retry connection "conn1" limited to 1 retry
          And a retry connection "conn2" limited to 1 retry
         When I query "SELECT 1" on "conn1"
          And I query "SELECT 1" on "conn2"
         Then the last query failed on "conn2"
          And the last error should be "CON_COUNT" on "conn2"
          And "conn2" retry limit should be 0

    Scenario: Get database
        Then I can get the database name on "conn"

    Scenario: No such table
        Given table "not_here_yet" can be created automatically on "conn"
         When I prepare a statement "SELECT * FROM not_here_yet" on "conn"
          And I execute this statement
         Then the last statement succeeded
          And the last error should be "NO_SUCH_TABLE" on "conn"
