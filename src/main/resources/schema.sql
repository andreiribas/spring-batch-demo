DROP TABLE IF EXISTS people;

CREATE TABLE people (
    person_id  BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50)
);
