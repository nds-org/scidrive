# ************************************************************
# Sequel Pro SQL dump
# Version 4004
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: vospace.sdsc.edu (MySQL 5.1.66)
# Database: vospace
# Generation Time: 2013-03-13 06:34:59 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table container_shares
# ------------------------------------------------------------

CREATE TABLE `container_shares` (
  `share_id` varchar(150) NOT NULL DEFAULT '',
  `container_id` int(11) unsigned NOT NULL,
  `group_id` int(11) unsigned DEFAULT NULL,
  `share_write_permission` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`share_id`),
  KEY `container_id` (`container_id`),
  KEY `group_id` (`group_id`),
  CONSTRAINT `container_shares_ibfk_1` FOREIGN KEY (`container_id`) REFERENCES `containers` (`container_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `container_shares_ibfk_2` FOREIGN KEY (`group_id`) REFERENCES `groups` (`group_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table containers
# ------------------------------------------------------------

CREATE TABLE `containers` (
  `container_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `container_name` varchar(128) NOT NULL DEFAULT '',
  `user_id` int(11) unsigned NOT NULL,
  PRIMARY KEY (`container_id`),
  UNIQUE KEY `name` (`container_name`,`user_id`),
  KEY `user_id` (`user_id`),
  KEY `container_name` (`container_name`),
  CONSTRAINT `containers_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table groups
# ------------------------------------------------------------

CREATE TABLE `groups` (
  `group_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `group_name` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`group_id`),
  UNIQUE KEY `group_name` (`group_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table jobs
# ------------------------------------------------------------

CREATE TABLE `jobs` (
  `id` char(36) NOT NULL,
  `user_id` int(11) unsigned NOT NULL,
  `starttime` datetime DEFAULT NULL,
  `endtime` datetime DEFAULT NULL,
  `state` enum('PENDING','RUN','COMPLETED','ERROR') NOT NULL,
  `direction` enum('PULLFROMVOSPACE','PULLTOVOSPACE','PUSHFROMVOSPACE','PUSHTOVOSPACE','LOCAL') NOT NULL,
  `target` text,
  `json_notation` text NOT NULL,
  `note` text,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `jobs_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table node_properties
# ------------------------------------------------------------

CREATE TABLE `node_properties` (
  `node_id` int(11) unsigned NOT NULL,
  `property_id` int(11) unsigned NOT NULL,
  `property_value` varchar(256) NOT NULL DEFAULT '',
  PRIMARY KEY (`node_id`,`property_id`),
  KEY `property_id` (`property_id`),
  CONSTRAINT `node_properties_ibfk_1` FOREIGN KEY (`property_id`) REFERENCES `properties` (`property_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `node_properties_ibfk_2` FOREIGN KEY (`node_id`) REFERENCES `nodes` (`node_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table nodes
# ------------------------------------------------------------

CREATE TABLE `nodes` (
  `node_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `container_id` int(11) unsigned NOT NULL,
  `parent_node_id` int(11) unsigned DEFAULT NULL,
  `path` varchar(128) NOT NULL DEFAULT '',
  `type` enum('NODE','DATA_NODE','LINK_NODE','CONTAINER_NODE','UNSTRUCTURED_DATA_NODE','STRUCTURED_DATA_NODE') NOT NULL DEFAULT 'NODE',
  `current_rev` tinyint(1) unsigned NOT NULL DEFAULT '1',
  `rev` int(32) unsigned NOT NULL DEFAULT '0',
  `deleted` tinyint(1) NOT NULL DEFAULT '0',
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `size` bigint(20) NOT NULL DEFAULT '0',
  `mimetype` varchar(256) NOT NULL DEFAULT '',
  PRIMARY KEY (`node_id`),
  KEY `container_id` (`container_id`),
  KEY `parent_node_id` (`parent_node_id`),
  CONSTRAINT `container_id` FOREIGN KEY (`container_id`) REFERENCES `containers` (`container_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `nodes_ibfk_1` FOREIGN KEY (`parent_node_id`) REFERENCES `nodes` (`node_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table oauth_accessors
# ------------------------------------------------------------

CREATE TABLE `oauth_accessors` (
  `request_token` varchar(32) DEFAULT NULL,
  `access_token` varchar(32) DEFAULT NULL,
  `token_secret` varchar(32) NOT NULL,
  `consumer_id` int(11) unsigned NOT NULL,
  `container_id` int(11) unsigned DEFAULT NULL,
  `authorized` bit(1) NOT NULL DEFAULT b'0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `accessor_write_permission` bit(1) NOT NULL DEFAULT b'1',
  UNIQUE KEY `request_token` (`request_token`),
  UNIQUE KEY `access_token` (`access_token`),
  KEY `container_id` (`container_id`),
  CONSTRAINT `oauth_accessors_ibfk_1` FOREIGN KEY (`container_id`) REFERENCES `containers` (`container_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table oauth_consumers
# ------------------------------------------------------------

CREATE TABLE `oauth_consumers` (
  `consumer_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `consumer_key` varchar(32) NOT NULL,
  `consumer_secret` varchar(32) NOT NULL,
  `consumer_description` varchar(256) NOT NULL,
  `callback_url` varchar(256) DEFAULT NULL,
  `container` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`consumer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `oauth_consumers` WRITE;
/*!40000 ALTER TABLE `oauth_consumers` DISABLE KEYS */;

INSERT INTO `oauth_consumers` (`consumer_id`, `consumer_key`, `consumer_secret`, `consumer_description`, `callback_url`, `container`)
VALUES
	(1,'sclient','ssecret','sample client',NULL,NULL),
	(2,'vosync','vosync_ssecret','vosync app',NULL,'vosync');

/*!40000 ALTER TABLE `oauth_consumers` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table properties
# ------------------------------------------------------------

CREATE TABLE `properties` (
  `property_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `property_uri` varchar(128) NOT NULL,
  `property_type` set('property','accepts','provides') NOT NULL DEFAULT 'property',
  `property_readonly` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`property_id`),
  UNIQUE KEY `property_uri` (`property_uri`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `properties` WRITE;
/*!40000 ALTER TABLE `properties` DISABLE KEYS */;

INSERT INTO `properties` (`property_id`, `property_uri`, `property_type`, `property_readonly`)
VALUES
	(4,'ivo://ivoa.net/vospace/core#contenttype','property',1),
	(7,'ivo://ivoa.net/vospace/core#date','property',1),
	(8,'ivo://ivoa.net/vospace/core#length','property',1);

/*!40000 ALTER TABLE `properties` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table storage_users_pool
# ------------------------------------------------------------

CREATE TABLE `storage_users_pool` (
  `username` varchar(60) NOT NULL DEFAULT '',
  `apikey` varchar(60) DEFAULT NULL,
  `user_id` int(11) unsigned DEFAULT NULL,
  PRIMARY KEY (`username`),
  UNIQUE KEY `user_id` (`user_id`),
  CONSTRAINT `storage_users_pool_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table user_groups
# ------------------------------------------------------------

CREATE TABLE `user_groups` (
  `user_id` int(11) unsigned NOT NULL,
  `group_id` int(11) unsigned NOT NULL,
  UNIQUE KEY `user_id` (`user_id`,`group_id`),
  KEY `group_id` (`group_id`),
  CONSTRAINT `user_groups_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `user_groups_ibfk_2` FOREIGN KEY (`group_id`) REFERENCES `groups` (`group_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table user_identities
# ------------------------------------------------------------

CREATE TABLE `user_identities` (
  `user_id` int(11) unsigned NOT NULL,
  `identity` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`user_id`,`identity`),
  UNIQUE KEY `identity` (`identity`),
  CONSTRAINT `user_identities_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table users
# ------------------------------------------------------------

CREATE TABLE `users` (
  `user_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `storage_credentials` tinyblob,
  `certificate` blob,
  `certificate_expiration` datetime DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `softlimit` int(10) unsigned NOT NULL DEFAULT '1024',
  `hardlimit` int(10) unsigned NOT NULL DEFAULT '2048',
  `service_credentials` blob,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
