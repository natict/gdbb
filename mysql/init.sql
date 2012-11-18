DROP PROCEDURE IF EXISTS `create_graph_tables`;
DROP PROCEDURE IF EXISTS `common_neighbors`;

DELIMITER $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_graph_tables`( IN name VARCHAR(16) )
BEGIN
 SET @edgesTable = CONCAT(name, '_edges');
 SET @nodesTable = CONCAT(name, '_nodes');
 SET @dropTable = 'DROP TABLE IF EXISTS ';
 SET @dropNodesStr = CONCAT(@dropTable, @nodesTable);
 SET @dropEdgesStr = CONCAT(@dropTable, @edgesTable);
 PREPARE dropNodes FROM @dropNodesStr;
 PREPARE dropEdges FROM @dropEdgesStr;
 EXECUTE dropEdges;
 EXECUTE dropNodes;
 SET @createNodesStr = CONCAT('CREATE TABLE ',@nodesTable,' (
 `id` MEDIUMINT UNSIGNED NOT NULL,
 `name` varchar(64) NOT NULL,
 PRIMARY KEY (`id`)
 )');
 PREPARE createNodes FROM @createNodesStr;
 EXECUTE createNodes;
 SET @fk1 = CONCAT('fk_',@edgesTable,'_id1');
 SET @fk2 = CONCAT('fk_',@edgesTable,'_id2');
 SET @createEdgesStr = CONCAT('CREATE TABLE ',@edgesTable,' (
 `id1` MEDIUMINT UNSIGNED NOT NULL,
 `id2` MEDIUMINT UNSIGNED NOT NULL,
 PRIMARY KEY (`id1`,`id2`),
 KEY `',@fk1,'` (`id1`),
 KEY `',@fk2,'` (`id2`),
 CONSTRAINT `',@fk1,'` FOREIGN KEY (`id1`) REFERENCES `',@nodesTable,'` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
 CONSTRAINT `',@fk2,'` FOREIGN KEY (`id2`) REFERENCES `',@nodesTable,'` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
 )');
 PREPARE createEdges FROM @createEdgesStr;
 EXECUTE createEdges;
END $$

CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `common_neighbors2`( IN name VARCHAR(16) )
BEGIN
SET @selectCN = CONCAT('
select e1.id1 as x, e2.id1 as y, count(*) as score
from ',name,'_edges as e1,
     ',name,'_edges as e2
where e1.id1<e2.id1 and e1.id2=e2.id2
group by x,y
order by score desc
limit 0,100;');

PREPARE cn FROM @selectCN;
EXECUTE cn;

END $$

CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `common_neighbors`( IN name VARCHAR(16) )
BEGIN
SET @selectCN = CONCAT('
select x,y,SUM(s) as score
from (
/*
* x,a   a,y
*/
(select e1.id1 as x, e2.id2 as y, count(*) as s
from ',name,'_edges as e1,
     ',name,'_edges as e2
where e1.id1<e2.id2 and e1.id2=e2.id1
group by x,y)
union all
/*
* x,a   y,a
*/
(select e1.id1 as x, e2.id1 as y, count(*) as s
from ',name,'_edges as e1,
     ',name,'_edges as e2
where e1.id1<e2.id1 and e1.id2=e2.id2
group by x,y)
union all
/*
* a,x   a,y
*/
(select e1.id2 as x, e2.id2 as y, count(*) as s
from ',name,'_edges as e1,
     ',name,'_edges as e2
where e1.id2<e2.id2 and e1.id1=e2.id1
group by x,y)) as t
group by t.x,t.y
order by score desc
limit 0,100;');

PREPARE cn FROM @selectCN;
EXECUTE cn;

END $$

DROP PROCEDURE IF EXISTS `create_cn_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_cn_table`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
DROP TABLE IF EXISTS `',name,'_cn`;
');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
CREATE TABLE ',name,'_cn (
  `id1` mediumint(8) unsigned NOT NULL,
  `id2` mediumint(8) unsigned NOT NULL,
  `id3` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id1`,`id2`,`id3`),
  KEY `fk_',name,'_pair` (`id1`, `id2`)
)');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
INSERT INTO `',name,'_cn`
select e1.id1 as x, e2.id1 as y, e1.id2 as n
from ',name,'_edges as e1
join ',name,'_edges as e2 on (e1.id2=e2.id2 and e1.id1<e2.id1);
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

DROP PROCEDURE IF EXISTS `create_cnc_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_cnc_table`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
DROP TABLE IF EXISTS `',name,'_cnc`;
');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
CREATE TABLE `',name,'_cnc` (
  `id1` mediumint(8) unsigned NOT NULL,
  `id2` mediumint(8) unsigned NOT NULL,
  `count` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id1`,`id2`)
)');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
INSERT INTO `',name,'_cnc`
select id1 as x, id2 as y, count(id3) as c
from ',name,'_cn
group by x,y;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

DROP PROCEDURE IF EXISTS `create_neighbors_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_neighbors_table`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
DROP TABLE IF EXISTS `',name,'_neighbors`;
');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
CREATE TABLE `',name,'_neighbors` (
  `id` mediumint(8) unsigned NOT NULL,
  `neighbors` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id`)
)');
PREPARE foo FROM @str;
EXECUTE foo;

SET @str = CONCAT('
INSERT INTO `',name,'_neighbors`
select id1 as x, count(id2) as n
from ',name,'_edges
group by x;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

# Common Neighbors
DROP PROCEDURE IF EXISTS `b_Common_Neighbors` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Common_Neighbors`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
SELECT
`',name,'_cnc`.`id1`,
`',name,'_cnc`.`id2`,
`',name,'_cnc`.`count`
FROM `gdbb`.`',name,'_cnc`
order by count desc
limit 0,100;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

# Jaccard's Coefficient
DROP PROCEDURE IF EXISTS `b_Jaccard_Coefficient` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Jaccard_Coefficient`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
select  cnc.id1 as x, 
        cnc.id2 as y, 
        cnc.count as i, 
        (n1.neighbors + n2.neighbors - cnc.count) as u, 
        cnc.count/(n1.neighbors + n2.neighbors - cnc.count) as score
from ',name,'_cnc as cnc
join ',name,'_neighbors as n1 on cnc.id1=n1.id
join ',name,'_neighbors as n2 on cnc.id2=n2.id
order by score desc
limit 0,100;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

# Adamic/Adar
DROP PROCEDURE IF EXISTS `b_Adamic_Adar` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Adamic_Adar`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
SELECT
`',name,'_cn`.`id1` as x,
`',name,'_cn`.`id2` as y,
SUM(1/(LOG10(`',name,'_neighbors`.`neighbors`))) as score
FROM `gdbb`.`',name,'_cn`
join `',name,'_neighbors` on (`',name,'_cn`.`id3` = `',name,'_neighbors`.`id`)
group by x, y
order by score desc
limit 0,100;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$

# Preferential attachment
DROP PROCEDURE IF EXISTS `b_Preferential_attachment` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Preferential_attachment`( IN name VARCHAR(16) )
BEGIN
SET @str = CONCAT('
select  cnc.id1 as x, 
        cnc.id2 as y, 
        (n1.neighbors * n2.neighbors) as score
from ',name,'_cnc as cnc
join ',name,'_neighbors as n1 on cnc.id1=n1.id
join ',name,'_neighbors as n2 on cnc.id2=n2.id
order by score desc
limit 0,100;
');
PREPARE foo FROM @str;
EXECUTE foo;
END $$
