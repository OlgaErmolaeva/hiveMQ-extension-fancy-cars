CREATE TABLE generation
(
id SERIAL PRIMARY KEY,
clientid VARCHAR(120) NOT NULL UNIQUE,
gen INTEGER NOT NULL
);

INSERT INTO generation(clientid, gen)
VALUES ('12_old_34',1),('56_new_78',2);