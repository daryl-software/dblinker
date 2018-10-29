CREATE TABLE db.`user` (
                         `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                         `name` varchar(50) NULL,
                         `email` varchar(128) NOT NULL,
                         PRIMARY KEY (`id`),
                         KEY `u_email` (`email`)
);

INSERT INTO db.`user` (name, email) VALUES
                                           ('John', 'john@yopmail.com'),
                                           ('Roger', 'roger@yopmail.com'),
                                           ('Max', 'max@yopmail.com');
