CREATE TABLE users (
                         name varchar(50) NULL,
                         email varchar(128) NOT NULL
);

INSERT INTO users (name, email) VALUES
('John', 'john@yopmail.com'),
('Roger', 'roger@yopmail.com'),
('Max', 'max@yopmail.com');

REVOKE ALL ON users FROM slave_user;
GRANT SELECT ON users TO slave_user;