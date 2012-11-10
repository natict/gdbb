/*
*	Init the gdbb database and user.
*	Running this script requires root user privileges
*	GRANT should automatically create the user 
*	(unless NO_AUTO_CREATE_USER)
*/
DROP DATABASE IF EXISTS gdbb;
CREATE DATABASE gdbb;
GRANT ALL ON gdbb.* TO 'gdbb'@'localhost';
