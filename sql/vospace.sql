# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: swiftnode01 (MySQL 5.6.12-log)
# Database: vobox
# Generation Time: 2013-09-06 20:22:39 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table chunked_uploads
# ------------------------------------------------------------

CREATE TABLE `chunked_uploads` (
  `chunk_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `chunked_name` char(15) NOT NULL DEFAULT '',
  `chunked_num` int(11) unsigned NOT NULL,
  `user_id` int(11) unsigned NOT NULL,
  `node_id` int(11) unsigned DEFAULT NULL,
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `size` int(11) unsigned NOT NULL,
  PRIMARY KEY (`chunk_id`),
  KEY `node_id` (`node_id`),
  KEY `chunked_name` (`chunked_name`),
  CONSTRAINT `chunked_uploads_ibfk_1` FOREIGN KEY (`node_id`) REFERENCES `nodes` (`node_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table container_shares
# ------------------------------------------------------------

CREATE TABLE `container_shares` (
  `share_key` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `share_id` varchar(150) NOT NULL DEFAULT '',
  `container_id` int(11) unsigned NOT NULL,
  `group_id` int(11) unsigned DEFAULT NULL,
  `share_write_permission` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`share_key`),
  UNIQUE KEY `share_id` (`share_id`),
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
  `property_value` text NOT NULL,
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
  `size` bigint(20) unsigned NOT NULL DEFAULT '0',
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
  `authorized` tinyint(1) NOT NULL DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `accessor_write_permission` tinyint(1) NOT NULL DEFAULT '1',
  `share_key` int(11) unsigned DEFAULT NULL,
  UNIQUE KEY `request_token` (`request_token`),
  UNIQUE KEY `access_token` (`access_token`),
  KEY `container_id` (`container_id`),
  KEY `share_key` (`share_key`),
  CONSTRAINT `oauth_accessors_ibfk_1` FOREIGN KEY (`container_id`) REFERENCES `containers` (`container_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `oauth_accessors_ibfk_2` FOREIGN KEY (`share_key`) REFERENCES `container_shares` (`share_key`) ON DELETE CASCADE ON UPDATE CASCADE
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
	(2,'vosync','vosync_ssecret','vosync app',NULL,'vosync'),
	(3,'test_cons','test_cons','test_cons',NULL,'test_cons'),
	(4,'casjobs','sbojsac','casjobs',NULL,'casjobs');

/*!40000 ALTER TABLE `oauth_consumers` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table oauth_nonces
# ------------------------------------------------------------

CREATE TABLE `oauth_nonces` (
  `timestamp` bigint(20) unsigned NOT NULL DEFAULT '0',
  `nonce` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`timestamp`,`nonce`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table properties
# ------------------------------------------------------------

CREATE TABLE `properties` (
  `property_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `property_uri` varchar(128) NOT NULL,
  `property_readonly` tinyint(1) NOT NULL DEFAULT '0',
  `property_accepts` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `property_provides` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`property_id`),
  UNIQUE KEY `property_uri` (`property_uri`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `properties` WRITE;
/*!40000 ALTER TABLE `properties` DISABLE KEYS */;

INSERT INTO `properties` (`property_id`, `property_uri`, `property_readonly`, `property_accepts`, `property_provides`)
VALUES
	(1,'ivo://ivoa.net/vospace/core#external_link',0,0,1),
	(2,'ivo://ivoa.net/vospace/core#contenttype',1,0,1),
	(3,'ivo://ivoa.net/vospace/core#date',1,0,1),
	(4,'ivo://ivoa.net/vospace/core#length',1,0,1),
	(5,'ivo://ivoa.net/vospace/core#description',0,1,0),
	(6,'ivo://ivoa.net/vospace/core#simulation_dataset',0,0,1),
	(7,'ivo://ivoa.net/vospace/core#simulation_id',0,0,1),
	(50,'ivo://ivoa.net/vospace/core#processing',0,0,0),
	(56,'ivo://ivoa.net/vospace/core#error_message',0,0,0);

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
  `user_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `storage_credentials` tinyblob,
  `certificate` blob,
  `certificate_expiration` datetime DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `softlimit` int(11) unsigned NOT NULL DEFAULT '1024',
  `hardlimit` int(11) unsigned NOT NULL DEFAULT '2048',
  `service_credentials` blob,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
