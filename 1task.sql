DROP DATABASE IF EXISTS db; 
CREATE DATABASE IF NOT EXISTS db;
USE db; 

CREATE TABLE IF NOT EXISTS cl 
	(TYPE TEXT, 
         SECCODE VARCHAR(40) PRIMARY KEY);

LOAD DATA INFILE '/Users/shatilova/Desktop/project/list-archive-01012015.csv' 
INTO TABLE cl
	FIELDS TERMINATED BY ';'
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 2 LINES
	(@d, @d, @d, @d, @d, @type, @d, @d, @d, @d, @seccode, @d, @d, @d)
	SET
	TYPE = @type,
	SECCODE = @seccode; 

CREATE TABLE IF NOT EXISTS class
	(TYPE VARCHAR(40), 
         SECCODE VARCHAR(40) PRIMARY KEY);

INSERT INTO class 
SELECT * FROM cl WHERE 
	cl.TYPE = 'Акция обыкновенная' OR 
	cl.TYPE = 'Акция привилегированная' OR 
	cl.TYPE = 'Облигация муниципальная' OR
	cl.TYPE = 'Облигация корпоративная' OR
	cl.TYPE = 'Облигация биржевая' OR
	cl.TYPE = 'Облигация субфедеральная' OR
	cl.TYPE = 'Облигации иностранного эмитента' OR
	cl.TYPE = 'Еврооблигация' OR
	cl.TYPE = 'Облигация федерального займа'; 

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Акция обыкновенная', 'o');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Акция привилегированная', 'p');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигация муниципальная', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигация корпоративная', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигация биржевая', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигация субфедеральная', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигации иностранного эмитента', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Еврооблигация', 'b');

UPDATE `class`
SET `TYPE` = replace(TYPE, 'Облигация федерального займа', 'b');

DROP TABLE cl; 

CREATE TABLE orders
	(NO BIGINT, 
	SECCODE VARCHAR(40),
	BUYSELL CHAR(1),
	TIME INT, 
	ORDERNO BIGINT,
	ACTION ENUM('0','1','2'),
	PRICE FLOAT, 
	VOLUME BIGINT,
	TRADENO BIGINT, 
	TRADEPRICE FLOAT);

LOAD DATA INFILE '/Users/shatilova/Desktop/project/OrderLog20150928.txt' 
INTO TABLE orders
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\r\n'
	IGNORE 1 LINES
	(NO, 
	@SECCODE,
	BUYSELL,
	TIME,
	ORDERNO,
	ACTION,
	PRICE,
	VOLUME,
	@TRADENO,
	@TRADEPRICE)
	SET
	SECCODE = IF(@SECCODE='', DEFAULT(SECCODE), @SECCODE),
	TRADENO = IF(@TRADENO='', DEFAULT(TRADENO), @TRADENO),
	TRADEPRICE = IF(@TRADEPRICE='', DEFAULT(TRADEPRICE), @TRADEPRICE);

CREATE TABLE ordinary
	(NO BIGINT PRIMARY KEY, 
	SECCODE VARCHAR(40),
	BUYSELL CHAR(1),
	TIME INT, 
	ORDERNO BIGINT,
	ACTION ENUM('0','1','2'),
	PRICE FLOAT, 
	VOLUME BIGINT,
	TRADENO BIGINT, 
	TRADEPRICE FLOAT, 
	FOREIGN KEY (SECCODE) REFERENCES class(SECCODE));

INSERT INTO ordinary 
SELECT o.* FROM orders o 
	INNER JOIN class c
	ON o.seccode = c.seccode
	AND c.TYPE = 'o';

CREATE TABLE privil
	(NO BIGINT PRIMARY KEY, 
	SECCODE VARCHAR(40),
	BUYSELL CHAR(1),
	TIME INT, 
	ORDERNO BIGINT,
	ACTION ENUM('0','1','2'),
	PRICE FLOAT, 
	VOLUME BIGINT,
	TRADENO BIGINT, 
	TRADEPRICE FLOAT, 
	FOREIGN KEY (SECCODE) REFERENCES class(SECCODE));

INSERT INTO privil 
SELECT o.* FROM orders o 
	INNER JOIN class c
	ON o.seccode = c.seccode
	AND c.TYPE = 'p';

CREATE TABLE bonds
	(NO BIGINT PRIMARY KEY, 
	SECCODE VARCHAR(40),
	BUYSELL CHAR(1),
	TIME INT, 
	ORDERNO BIGINT,
	ACTION ENUM('0','1','2'),
	PRICE FLOAT, 
	VOLUME BIGINT,
	TRADENO BIGINT, 
	TRADEPRICE FLOAT, 
	FOREIGN KEY (SECCODE) REFERENCES class(SECCODE));

INSERT INTO bonds 
SELECT o.* FROM orders o 
	INNER JOIN class c
	ON o.seccode = c.seccode
	AND c.TYPE = 'b';

# Query...

CREATE INDEX index_tradeno
	ON ordinary(tradeno);
	
	
SELECT q.seccode, count(q.seccode) as count FROM
	(SELECT b.seccode as seccode, b.buysell as b_buysell, b.time as b_time, 
		b.orderno as b_orderno, b.action as b_action, b.volume as b_volume, 
		b.tradeno as b_tradeno, b.tradeprice as b_tradeprice, 
			s.volume as s_volume, s.action as s_action, s.orderno as s_orderno, 
			s.time as s_time, s.buysell as s_buysell FROM
				ordinary b
				INNER JOIN ordinary s ON
					b.buysell = 'B' AND
					s.buysell = 'S' AND 
					b.tradeno = s.tradeno WHERE b.action = '2' AND s.action = '2') q
	GROUP BY q.seccode ORDER BY count(q.seccode) DESC limit 3;




