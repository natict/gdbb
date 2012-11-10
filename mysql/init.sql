DROP PROCEDURE IF EXISTS `create_graph_tables`;

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
END
