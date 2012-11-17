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
 `id` int(11) UNSIGNED NOT NULL,
 `name` varchar(64) NOT NULL,
 PRIMARY KEY (`id`)
 )');
 PREPARE createNodes FROM @createNodesStr;
 EXECUTE createNodes;
 SET @fk1 = CONCAT('fk_',@edgesTable,'_id1');
 SET @fk2 = CONCAT('fk_',@edgesTable,'_id2');
 SET @createEdgesStr = CONCAT('CREATE TABLE ',@edgesTable,' (
 `id1` int(11) UNSIGNED NOT NULL,
 `id2` int(11) UNSIGNED NOT NULL,
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
