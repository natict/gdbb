DELIMITER $$

DROP PROCEDURE IF EXISTS `create_graph_tables`;
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_graph_tables`()
BEGIN
 DROP TABLE IF EXISTS nodes;
 CREATE TABLE nodes (
  `id` MEDIUMINT UNSIGNED NOT NULL,
  `name` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
 );

 DROP TABLE IF EXISTS edges;
 CREATE TABLE edges (
  `id1` MEDIUMINT UNSIGNED NOT NULL,
  `id2` MEDIUMINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id1`,`id2`),
  KEY `fk_edges_id1` (`id1`),
  KEY `fk_edges_id2` (`id2`)
 );
END $$

DROP PROCEDURE IF EXISTS `create_cn_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_cn_table`()
BEGIN
 DROP TABLE IF EXISTS `cn`;

 CREATE TABLE cn (
  `id1` mediumint(8) unsigned NOT NULL,
  `id2` mediumint(8) unsigned NOT NULL,
  `id3` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id1`,`id2`,`id3`),
  KEY `fk_pair` (`id1`, `id2`)
 );

 INSERT INTO `cn`
  select e1.id1 as x, e2.id1 as y, e1.id2 as n
  from edges as e1
  join edges as e2 on (e1.id2=e2.id2 and e1.id1<e2.id1);
END $$

DROP PROCEDURE IF EXISTS `create_cnc_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_cnc_table`()
BEGIN
 DROP TABLE IF EXISTS `cnc`;

 CREATE TABLE `cnc` (
  `id1` mediumint(8) unsigned NOT NULL,
  `id2` mediumint(8) unsigned NOT NULL,
  `count` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id1`,`id2`)
 );

 INSERT INTO `cnc`
  select id1 as x, id2 as y, count(id3) as c
  from cn
  group by x,y;
END $$

DROP PROCEDURE IF EXISTS `create_neighbors_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_neighbors_table`()
BEGIN
 DROP TABLE IF EXISTS `neighbors`;

 CREATE TABLE `neighbors` (
  `id` mediumint(8) unsigned NOT NULL,
  `neighbors` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id`)
 );

 INSERT INTO `neighbors`
  select id1 as x, count(id2) as n
  from edges
  group by x;
END $$

DROP PROCEDURE IF EXISTS `create_topn_table` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `create_topn_table`()
BEGIN
 DROP TABLE IF EXISTS `topn`;

 CREATE TABLE `topn` (
  `id` mediumint(8) unsigned NOT NULL,
  `neighbors` mediumint(8) unsigned NOT NULL,
  PRIMARY KEY (`id`)
 );

 INSERT INTO `topn`
  select id, neighbors
  from neighbors
  order by neighbors desc
  limit 101;
END $$


# Common Neighbors
DROP PROCEDURE IF EXISTS `b_Common_Neighbors` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Common_Neighbors`()
BEGIN
	SELECT `cnc`.`id1`, `cnc`.`id2`, `cnc`.`count`
	FROM `gdbb`.`cnc`
	order by count desc
	limit 0,100;
END $$

# Jaccards Coefficient
DROP PROCEDURE IF EXISTS `b_Jaccard_Coefficient` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Jaccard_Coefficient`()
BEGIN
	select cnc.id1 as x, 
		cnc.id2 as y, 
		cnc.count as i, 
		(n1.neighbors + n2.neighbors - cnc.count) as u, 
		cnc.count/(n1.neighbors + n2.neighbors - cnc.count) as score
	from cnc as cnc
		join neighbors as n1 on cnc.id1=n1.id
		join neighbors as n2 on cnc.id2=n2.id
	order by score desc
	limit 0,100;
END $$

# Adamic/Adar
DROP PROCEDURE IF EXISTS `b_Adamic_Adar` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Adamic_Adar`()
BEGIN
	SELECT `cn`.`id1` as x,
		`cn`.`id2` as y,
		SUM(1/(LOG10(`neighbors`.`neighbors`))) as score
	FROM `gdbb`.`cn`
		join `neighbors` on (`cn`.`id3` = `neighbors`.`id`)
	group by x, y
	order by score desc
	limit 0,100;
END $$

# Preferential attachment
DROP PROCEDURE IF EXISTS `b_Preferential_attachment` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `b_Preferential_attachment`()
BEGIN
	select t1.id as x, 
		t2.id as y, 
		t1.neighbors * t2.neighbors as s 
		from topn as t1, topn as t2 
		where t1.id < t2.id 
		order by s desc 
		limit 100;
END $$

# Common Neighbors for given node x
DROP PROCEDURE IF EXISTS `x_Common_Neighbors` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `x_Common_Neighbors`(IN x mediumint(8) unsigned)
BEGIN
	select e2.id2 as y, count(e2.id1) as cn
	from edges as e1 
		join edges as e2 on (e1.id1 = x and 
								e1.id2 = e2.id1 and 
								e1.id1 <> e2.id2)
	group by y
	order by cn desc;
END $$

# Jaccards Coefficient for given node x
DROP PROCEDURE IF EXISTS `x_Jaccard_Coefficient` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `x_Jaccard_Coefficient`(IN x mediumint(8) unsigned)
BEGIN
	select t.y as y, 
		   n1.neighbors as yn,
		   n2.neighbors as xn,
		   t.cn as cn,
		   (t.cn/(n2.neighbors+n1.neighbors-t.cn)) as  s
	from (select e2.id2 as y, count(e2.id1) as cn
		from edges as e1 
			join edges as e2 on (e1.id1 = x and 
									e1.id2 = e2.id1 and 
									e1.id1 <> e2.id2)
		group by y) as t
		join neighbors as n1 on (n1.id = t.y)
		join neighbors as n2 on (n2.id = x)
	order by s desc;
END $$

#  Adamic/Adar for given node x
DROP PROCEDURE IF EXISTS `x_Adamic_Adar` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `x_Adamic_Adar`(IN x mediumint(8) unsigned)
BEGIN
	select e2.id2 as y, 
    	SUM(1/LOG10(n.neighbors)) as s
	from edges as e1 
		join edges as e2 on (e1.id1 = x and 
								e1.id2 = e2.id1 and 
								e1.id1 <> e2.id2) 
		join neighbors as n on (n.id = e2.id1)
	group by y
	order by s desc;
END $$

# Preferential attachment for given node x
DROP PROCEDURE IF EXISTS `x_Preferential_attachment` $$
CREATE DEFINER=`gdbb`@`localhost` PROCEDURE `x_Preferential_attachment`(IN x mediumint(8) unsigned)
BEGIN
	select t.id as y, 
    	(t.neighbors * n.neighbors) as s
	from topn as t,
		neighbors as n
	where n.id = x and t.id <> x
	order by s desc
	limit 100;
END $$


# Graph Distance for a given x
#   recursive version
#   usage: 
#   to print top 100 nodes: call GraphDistance(6, 4, 100);
#   to generate gdtmp table: call GraphDistance(6, 4, -1); 
# NOTE:
#   you must: set max_sp_recursion_depth=10;
drop procedure if exists x_Graph_Distance $$
CREATE PROCEDURE x_Graph_Distance(IN n mediumint(8) unsigned, IN depth int , IN lim int)
begin
  if (depth = 1) then 
    DROP TABLE IF EXISTS gdtmp;
    CREATE TABLE gdtmp (y mediumint(8) unsigned, l int, PRIMARY KEY (`y`));
    insert into gdtmp select e1.id2 as y, -(depth) as l from edges as e1 where e1.id1=n;   
  elseif (depth > 1) then 
    call x_Graph_Distance(n, depth - 1, -1);
    insert ignore into gdtmp select e.id2 as y, -(depth) as l from edges as e join gdtmp as g on (e.id1 = g.y and g.l = -(depth-1) and e.id2 <> n);
  end if;
  if (lim >= 0 and depth >= 1) then
    select * from gdtmp order by l desc limit lim;
    DROP TABLE IF EXISTS gdtmp;
  end if;
end $$


# Katz (unweighted) for a given x
# usage:
#   call x_Katz(6, 4, 0.1, 100);
# NOTE:
#   run-time grows exponentially with depth due to the size of ptmp table (works in seconds for depth<6)
drop procedure if exists x_Katz $$
CREATE PROCEDURE x_Katz(IN n mediumint(8) unsigned, IN depth int , IN beta double, IN lim int unsigned)
begin
  if (depth = 1) then 
    DROP TABLE IF EXISTS ptmp;
    CREATE TABLE ptmp (y mediumint(8) unsigned, l int);
    insert into ptmp select e1.id2 as y, depth as l from edges as e1 where e1.id1=n;   
  elseif (depth > 1) then 
    call x_Katz(n, depth - 1, 0, 0);
    insert into ptmp select e.id2 as y, depth as l from edges as e join ptmp as g on (e.id1 = g.y and g.l = depth - 1);
  end if;
  if (beta > 0 and depth >= 1) then
    select y, SUM(c*POW(beta, l)) as s from (select y, l, count(*) as c from ptmp group by y,l) as t where y<> n group by y order by s desc limit lim;
    DROP TABLE IF EXISTS ptmp;
  end if;
end $$

# Rooted PageRank for a given x
# usage:
#   call x_RootedPageRank(6);
drop procedure if exists x_RootedPageRank $$
CREATE PROCEDURE x_RootedPageRank(IN n mediumint(8) unsigned)
BEGIN
  DROP TABLE IF EXISTS `RootedPageRankTemp`;
  CREATE TABLE `RootedPageRankTemp` (
      `id` mediumint(8) unsigned NOT NULL,
      `rpr` float,
      `nrpr` float,
      PRIMARY KEY (`id`)
  );
  SET @d = 0.85; # damping factor
  SELECT count(*) FROM nodes into @node_count;
  # init rpr to 1/N
  INSERT INTO `RootedPageRankTemp`
    select id1 as `id`, NULL as `rpr`, 1.0/@node_count as `nrpr`
    from edges
    group by id;
  SET @mcount = 0;
  # update rpr until top-100 nodes converge
  REPEAT
      REPLACE `RootedPageRankTemp`
        select rpr.id as id, rpr.nrpr as rpr, rpr.nrpr as nrpr
        from  `RootedPageRankTemp` as rpr;
      REPLACE `RootedPageRankTemp`
        select rpr1.id as id, rpr1.rpr as rpr, (SUM(rpr2.rpr/n.neighbors)*@d) as nrpr
        from  `RootedPageRankTemp` as rpr1,
              edges as e,
              neighbors as n,
              `RootedPageRankTemp` as rpr2
        where rpr1.id = e.id1 and n.id = e.id2 and rpr2.id = e.id2
        group by rpr1.id;
      UPDATE `RootedPageRankTemp` SET nrpr = nrpr + (1-@d)/@node_count WHERE id = n;
      select count(*)
      from
          (SELECT @rownum := @rownum + 1 AS rn,rpr.id from `RootedPageRankTemp` rpr,(SELECT @rownum:=0) rnfoo ORDER BY `nrpr` desc LIMIT 100) newrprtop, 
          (SELECT @rownum2 := @rownum2 + 1 AS rn,rpr.id from `RootedPageRankTemp` rpr,(SELECT @rownum2:=0) rnfoo ORDER BY `rpr` desc LIMIT 100) oldrprtop
      where newrprtop.rn = oldrprtop.rn and newrprtop.id = oldrprtop.id
      into @mcount;
  UNTIL @mcount = 100 END REPEAT;
  # return top-100 nodes
  SELECT id, nrpr from `RootedPageRankTemp` order by nrpr desc limit 100;
END $$
