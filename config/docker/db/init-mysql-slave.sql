CREATE TABLE db.`users` (
  `name` varchar(50) NULL,
  `email` varchar(128) NOT NULL
);

INSERT INTO db.`users` (name, email) VALUES
                         ('John', 'john@yopmail.com'),
                         ('Roger', 'roger@yopmail.com'),
                         ('Max', 'max@yopmail.com');

REVOKE ALL ON db.* FROM 'slave_user'@'%';
GRANT SELECT ON *.* TO 'slave_user'@'%';

FLUSH PRIVILEGES ;