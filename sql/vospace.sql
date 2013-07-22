# ************************************************************
# Sequel Pro SQL dump
# Version 4004
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: zinc26.pha.jhu.edu (MySQL 5.1.69-log)
# Database: vospace_20_2
# Generation Time: 2013-06-27 09:18:57 +0000
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
  `share_id` varchar(150) NOT NULL DEFAULT '',
  `container_id` int(11) unsigned NOT NULL,
  `group_id` int(11) unsigned DEFAULT NULL,
  `share_write_permission` tinyint(1) NOT NULL DEFAULT '0',
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
  `property_value` text NOT NULL,
  PRIMARY KEY (`node_id`,`property_id`),
  KEY `property_id` (`property_id`),
  CONSTRAINT `node_properties_ibfk_1` FOREIGN KEY (`property_id`) REFERENCES `properties` (`property_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `node_properties_ibfk_2` FOREIGN KEY (`node_id`) REFERENCES `nodes` (`node_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `node_properties` WRITE;
/*!40000 ALTER TABLE `node_properties` DISABLE KEYS */;

INSERT INTO `node_properties` (`node_id`, `property_id`, `property_value`)
VALUES
	(42,8,'FITS Files Container'),
	(43,8,'Images'),
	(44,8,''),
	(221,4,'bla-bla'),
	(274,8,'m31 image'),
	(488,23,'ada061d3-a13a-4961-a5ef-0eae2f999bff 86fb04c9-224a-4f2b-8f40-a2fc2a692315 fb48a98e-06ce-40cb-bb1c-f8211cd5a0ba 04d67db3-e2a0-4e26-bfb5-01bf36b206b4 07c969b9-254d-4175-86a6-35a16a64735b 3ecb84f4-0e8f-40d2-813f-8ba8f19f07bf 30e8b984-6bee-4977-b167-80f093b5ed1e c406162f-de65-4f92-a1f7-55fa38769004 f62361b9-9d76-4687-b3b9-9b18b64b7f4f 8e2c4e2a-f7a4-453e-a64f-5171b4dd82c0 689aca6e-df08-4c84-b290-dda89716169e f000dede-bbc6-4e6e-95c9-70038bed2bc7 5aa1dd10-59d2-4019-a4c2-e4f8d1c9f257 74207b59-ab00-4663-9fa7-17b999a7c607 e814b88c-da70-43c6-bcf2-3509b99e4ceb 5fe17f3f-51bf-420a-a2a5-f90937c43407 851b74d8-d798-4a67-958b-157202a81c88 47e6f8fa-1bc1-47ab-9777-84a1c67b1e4f 17890401-8be7-45d0-a924-82041ab9b20d 9fdd639b-bd14-4e5d-9529-34c3d7575e24 75fc09ad-58c7-48d3-997b-671ffe9e1852 507a0e91-4711-44a7-b844-50f5c7efd4a4 0e1a3202-5f6d-4c87-86d5-898be7f3795f c691b52d-4f80-41f7-9573-484052ce69e3 b5030223-8bd8-4cff-bf7a-bf8b14b83f34 f953c44f-c6da-4999-8e1b-d41fb7538cbe 03d002f2-dbb6-4adb-ad5a-b755da25cc4e c4d5d4ae-eee3-422b-874d-cb85428a01ac b2c3a0a4-5fb1-4ab2-bcec-37e5564b7072 c7e3e8bf-a6f9-468e-adf6-bf764e2f045d 288bcc6c-7f68-46bf-8787-464f72024628 7e1d0556-72e5-4d4b-9085-0f7576e52fa3 93a490ef-ae63-4a64-bb27-c175530967aa 339bdf23-c0b7-4a09-b18a-2502febaf4b8 1f1f7616-1c33-4393-aa04-e10724810070 a0c78277-5fa2-4df9-ae19-bf7b5ea606da c4e508c2-0792-4c8b-ae3d-0c7bc7ce95e2 3fbec650-1473-4489-bf2f-11c16673f9d9 a489e83b-3735-4695-8973-3639e018b2e0 05576bb3-c3d9-4f0a-9443-09afc58be882 df7c7707-6064-4e16-8ac2-ec39cbb45817 d1791730-83a4-4187-b1bf-6629419c9679 be8cc089-b5f4-4660-bf4e-c8271381698c 7a5bec4c-54cd-4743-a78b-f73442f80099 c5e3c508-0030-484f-88e0-bb78daf55b26 a5992a19-abf6-455b-8a69-1a547b9b727f 876fbbe0-eab7-4dad-960f-39c94c568628 a2e994ed-ef02-4871-818b-0c41d9882373 2b3d6da9-6751-4701-aab1-4bd6f80ed52d 2c8538e2-11ce-4586-a2fe-06ea00e46b15 390e4bdd-ca8b-416d-8392-6c19882a36b6 2291fef4-1c1c-4f57-a102-9753d3e1759e 00b79354-0060-4f5d-b91b-36a78dae9ec7 14e749ce-9565-4d3c-aa90-7ac8af840025 9b503162-ed53-43d1-8ee9-ebc0c4531496 8245177c-5842-4c68-8656-9c6d9c6a13f3 200f16ef-1ec7-4c37-9549-6adbd65a33c7'),
	(488,24,'8d3f46fb-5f61-4dd9-b486-7f3e7da124c8'),
	(552,25,'http://skyserver.sdss3.org/CasJobs/MyDB.aspx?ObjName=Table_224_2&ObjType=TABLE&context=MyDB&type=normal http://skyserver.sdss3.org/CasJobs/MyDB.aspx?ObjName=Table_224_3&ObjType=TABLE&context=MyDB&type=normal');

/*!40000 ALTER TABLE `node_properties` ENABLE KEYS */;
UNLOCK TABLES;


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
  `authorized` tinyint(1) NOT NULL DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `accessor_write_permission` tinyint(1) NOT NULL DEFAULT '1',
  `is_share_token` tinyint(1) NOT NULL DEFAULT '0',
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
	(2,'vosync','vosync_ssecret','vosync app',NULL,'vosync'),
	(3,'test_cons','test_cons','test_cons',NULL,'test_cons');

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
