-- MySQL dump 10.13  Distrib 8.0.45, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: minor_project
-- ------------------------------------------------------
-- Server version	9.5.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN= 0;

--
-- GTID state at the beginning of the backup 
--

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'f67ca9f5-e0b7-11f0-b6e0-08bfb8684890:1-2756';

--
-- Table structure for table `batch_subjects`
--

DROP TABLE IF EXISTS `batch_subjects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `batch_subjects` (
  `batches_id` bigint NOT NULL,
  `subject_id` bigint NOT NULL,
  PRIMARY KEY (`batches_id`,`subject_id`),
  KEY `FK2oq539c40814bj4evihdbqvip` (`subject_id`),
  CONSTRAINT `FK2oq539c40814bj4evihdbqvip` FOREIGN KEY (`subject_id`) REFERENCES `subjects` (`id`),
  CONSTRAINT `FK77i1sj223bdfy3xbm0jnuax3j` FOREIGN KEY (`batches_id`) REFERENCES `batches` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `batch_subjects`
--

LOCK TABLES `batch_subjects` WRITE;
/*!40000 ALTER TABLE `batch_subjects` DISABLE KEYS */;
INSERT INTO `batch_subjects` VALUES (1,2),(2,2),(3,2),(3,5),(1,6),(2,6),(1,10),(2,10),(3,10),(1,11),(2,11),(3,11),(1,14),(2,14),(3,14),(1,15),(2,15),(3,15),(4,23),(5,23),(6,23),(4,24),(5,24),(6,24),(4,25),(5,25),(6,25),(4,26),(5,26),(6,26),(4,27),(5,27),(6,27),(4,28),(5,28),(6,28),(7,37),(8,37),(9,37),(7,38),(8,38),(9,38),(7,39),(8,39),(9,39),(7,40),(8,40),(9,40),(7,41),(8,41),(9,41),(7,42),(8,42),(9,42),(7,43),(8,43),(9,43);
/*!40000 ALTER TABLE `batch_subjects` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `batches`
--

DROP TABLE IF EXISTS `batches`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `batches` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(120) NOT NULL,
  `semester` int NOT NULL,
  `department_id` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_batch_dept_sem_name` (`department_id`,`semester`,`name`),
  CONSTRAINT `fk_batch_department` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `batches`
--

LOCK TABLES `batches` WRITE;
/*!40000 ALTER TABLE `batches` DISABLE KEYS */;
INSERT INTO `batches` VALUES (10,'A',1,1),(11,'B',1,1),(12,'BC',1,1),(1,'A',2,1),(2,'B',2,1),(3,'BC',2,1),(4,'A',4,1),(5,'B',4,1),(6,'BC',4,1),(7,'A',6,1),(8,'B',6,1),(9,'BC',6,1);
/*!40000 ALTER TABLE `batches` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `departments`
--

DROP TABLE IF EXISTS `departments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `departments` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `abbreviation` varchar(20) NOT NULL,
  `name` varchar(120) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKq6v6nnrch3oi9l7t9on9ik3l8` (`abbreviation`),
  UNIQUE KEY `UKj6cwks7xecs5jov19ro8ge3qk` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `departments`
--

LOCK TABLES `departments` WRITE;
/*!40000 ALTER TABLE `departments` DISABLE KEYS */;
INSERT INTO `departments` VALUES (1,'CS','Computer Science'),(2,'MAB','Mathematics'),(3,'PHY','Physics'),(4,'CHM','Chemistry'),(5,'HUM','Humanities'),(6,'AI','Artificial Intelligence');
/*!40000 ALTER TABLE `departments` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `rooms`
--

DROP TABLE IF EXISTS `rooms`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rooms` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `is_lab` bit(1) NOT NULL,
  `name` varchar(120) NOT NULL,
  `department_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_room_name` (`name`),
  KEY `fk_room_department` (`department_id`),
  CONSTRAINT `fk_room_department` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rooms`
--

LOCK TABLES `rooms` WRITE;
/*!40000 ALTER TABLE `rooms` DISABLE KEYS */;
INSERT INTO `rooms` VALUES (1,_binary '\0','Room 207',1),(2,_binary '\0','Room 206',1),(3,_binary '\0','Room 105',1),(4,_binary '\0','Room 104',1),(5,_binary '\0','Room 103',1),(6,_binary '\0','Room 209',1),(7,_binary '\0','Room 217',1),(8,_binary '','Chemistry Lab',4),(9,_binary '','Physics Lab',3),(10,_binary '','VVNCC Lab-1 (Common)',NULL),(11,_binary '','VVNCC Lab-2 (Common)',NULL),(12,_binary '','MCA Lab',1),(13,_binary '','CSE lab',1),(14,_binary '','Lab 216',5);
/*!40000 ALTER TABLE `rooms` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `subject_departments`
--

DROP TABLE IF EXISTS `subject_departments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `subject_departments` (
  `subject_id` bigint NOT NULL,
  `department_id` bigint NOT NULL,
  PRIMARY KEY (`subject_id`,`department_id`),
  KEY `FKpkndhww7s80vnuqhcd0p187uf` (`department_id`),
  CONSTRAINT `FK2gg6yudo65hhxr8mfh1k4kw9c` FOREIGN KEY (`subject_id`) REFERENCES `subjects` (`id`),
  CONSTRAINT `FKpkndhww7s80vnuqhcd0p187uf` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subject_departments`
--

LOCK TABLES `subject_departments` WRITE;
/*!40000 ALTER TABLE `subject_departments` DISABLE KEYS */;
INSERT INTO `subject_departments` VALUES (8,1),(1,2),(2,2),(3,2),(4,2),(6,3),(5,4),(7,5),(12,5);
/*!40000 ALTER TABLE `subject_departments` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `subjects`
--

DROP TABLE IF EXISTS `subjects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `subjects` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `lecture` int NOT NULL,
  `name` varchar(120) NOT NULL,
  `practical` int NOT NULL,
  `tutorial` int NOT NULL,
  `department_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKaodt3utnw0lsov4k9ta88dbpr` (`name`),
  KEY `fk_subject_department` (`department_id`),
  CONSTRAINT `fk_subject_department` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=55 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subjects`
--

LOCK TABLES `subjects` WRITE;
/*!40000 ALTER TABLE `subjects` DISABLE KEYS */;
INSERT INTO `subjects` VALUES (1,4,'Calculus and Algebra',0,0,2),(2,4,'Probability Distributions and Differential Equations',0,0,2),(3,4,'Discrete Mathematics',0,0,2),(4,4,'Numerical Method & Integral Transforms',0,0,2),(5,3,'Chemistry',3,0,4),(6,3,'Physics',3,0,3),(7,2,'Communication and Report Writing',1,0,5),(8,3,'Introduction to Computer Science and Programming',2,0,NULL),(9,3,'Digital Electronics',0,0,NULL),(10,3,'Problem Solving using Data Structures',2,0,NULL),(11,2,'Python Programming',2,0,NULL),(12,0,'Interactive Presentation Skills',2,0,NULL),(13,0,'Universal Human Values',0,0,NULL),(14,3,'Principle of System Software',0,0,NULL),(15,0,'Computer Workshop',2,0,NULL),(16,0,'Professional Ethics and Social Responsibility',0,0,NULL),(17,3,'Analysis and Design of Algorithm',2,0,NULL),(18,3,'Object Oriented Programming',2,0,NULL),(19,3,'Operating System',2,0,NULL),(20,3,'Computer System Organization',0,0,NULL),(21,0,'Internet Programming',3,0,NULL),(22,5,'Internship-1',0,0,NULL),(23,3,'Computer Networks',2,0,NULL),(24,3,'Database Management System',2,0,NULL),(25,3,'Automata and Compiler Design',2,0,NULL),(26,4,'Software Engineering',0,0,NULL),(27,3,'Foundation of Blockchain Technology',0,0,NULL),(28,0,'Advanced Java Programming Lab',4,0,NULL),(29,3,'Artificial Intelligence',0,0,NULL),(30,3,'Distributed System',0,0,NULL),(31,4,'Computer Graphics and Multimedia',0,0,NULL),(32,3,'Web Technology',0,0,NULL),(33,3,'Data Science',0,0,NULL),(34,0,'Programming Lab-1',4,0,NULL),(35,0,'Programming Lab-2',4,0,NULL),(36,3,'Internship-2',0,0,NULL),(37,3,'Cryptography and Network Security',0,0,NULL),(38,4,'Cloud Computing',0,0,NULL),(39,3,'Machine Learning',0,0,NULL),(40,3,'Advance Web Technology',0,0,NULL),(41,3,'Soft Computing',0,0,NULL),(42,0,'Programming Lab-3',4,0,NULL),(43,0,'Minor Project',2,0,NULL),(44,3,'Introduction to Smart Contract',0,0,NULL),(45,3,'Foundation of Data Science',0,0,NULL),(46,3,'Deep Learning',0,0,NULL),(47,3,'OOAD',0,0,NULL),(48,3,'Android Programming',0,0,NULL),(49,3,'Data Engineering and Analytics',0,0,NULL),(50,0,'Major Project Prelim',8,0,NULL),(51,0,'Programming Lab-4',0,0,NULL),(52,0,'Internship-3',2,0,NULL),(53,3,'Distributed Ledger Technology',0,0,NULL),(54,0,'Major Project',20,0,NULL);
/*!40000 ALTER TABLE `subjects` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `teacher_specializations`
--

DROP TABLE IF EXISTS `teacher_specializations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `teacher_specializations` (
  `teacher_id` bigint NOT NULL,
  `subject_id` bigint NOT NULL,
  PRIMARY KEY (`teacher_id`,`subject_id`),
  KEY `FKrxjj451j4r1q7qavp5dyt3hn8` (`subject_id`),
  CONSTRAINT `FK7kxov5462ma8j64xmyip7cg6o` FOREIGN KEY (`teacher_id`) REFERENCES `teachers` (`id`),
  CONSTRAINT `FKrxjj451j4r1q7qavp5dyt3hn8` FOREIGN KEY (`subject_id`) REFERENCES `subjects` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `teacher_specializations`
--

LOCK TABLES `teacher_specializations` WRITE;
/*!40000 ALTER TABLE `teacher_specializations` DISABLE KEYS */;
INSERT INTO `teacher_specializations` VALUES (14,1),(15,1),(16,1),(17,1),(18,1),(14,2),(15,2),(16,2),(17,2),(18,2),(14,3),(15,3),(16,3),(17,3),(18,3),(14,4),(15,4),(16,4),(17,4),(18,4),(22,5),(23,5),(24,5),(25,5),(19,6),(20,6),(21,6),(26,7),(27,7),(10,11),(28,11),(26,12),(27,12),(6,14),(13,14),(30,14),(29,15),(31,15),(2,20),(2,23),(4,23),(1,24),(34,24),(13,25),(32,25),(33,26),(8,27),(11,27),(10,28),(29,28),(8,38),(12,39),(34,39),(5,40),(7,40),(3,41),(36,41),(5,42),(8,42),(29,42),(9,44),(11,44);
/*!40000 ALTER TABLE `teacher_specializations` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `teachers`
--

DROP TABLE IF EXISTS `teachers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `teachers` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `abbreviation` varchar(20) NOT NULL,
  `name` varchar(120) NOT NULL,
  `department_id` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_teacher_department` (`department_id`),
  CONSTRAINT `fk_teacher_department` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=72 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `teachers`
--

LOCK TABLES `teachers` WRITE;
/*!40000 ALTER TABLE `teachers` DISABLE KEYS */;
INSERT INTO `teachers` VALUES (1,'KS','Dr. Kanak Saxsena',1),(2,'SP','Satish Pawar',1),(3,'PJ','Pranita Jain',1),(4,'UT','Usha Tigga',1),(5,'MA','Mukesh Azad',1),(6,'SD','Sumeet Dhillon',1),(7,'DRS','Divya Rishi Sahu',1),(8,'JS','Jyoti Soukar',1),(9,'GJ','Garima Jain',1),(10,'RS','Ruchi Shrivastava',1),(11,'RT','Ruchi Thakur',1),(12,'NM','Nupur Modh',1),(13,'RS','Ruchi Saxena',1),(14,'SHJ','Shailesh Jaloree',2),(15,'RD','Rajendra Dubey',2),(16,'VS','Vinita Singh',2),(17,'PS','Poonamlata Sagar',2),(18,'RKP','Rajendra Kumar Pathak',2),(19,'RJ','Ravi Jain',3),(20,'JP','Jitendra Parashar',3),(21,'SKM','SK Mahajan',3),(22,'MD','Manoj Datar',4),(23,'SWD','Swati Dubey',4),(24,'NR','Namrata Rajawat',4),(25,'RS','Raje Seugar',4),(26,'AS','Amitosh Singh',5),(27,'AD','Aditi Dwivedi',5),(28,'RK','Rashi Kumar',6),(29,'AY','Apoorva Yadav',6),(30,'SD','Smriti Dubey',6),(31,'SK','Sheena Kumar',6),(32,'SJ','Dr. Sunil Joshi',6),(33,'AM','Ankur Mishra',6),(34,'DS','Deepika Sharma',6),(35,'VD','Dr. Veena Datar',6),(36,'SC','Shaila Chugh',6);
/*!40000 ALTER TABLE `teachers` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `time_slots`
--

DROP TABLE IF EXISTS `time_slots`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `time_slots` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `day` varchar(16) NOT NULL,
  `end_time` time NOT NULL,
  `start_time` time NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKnuk4nq6pbo2wx24jjv1eu1nsy` (`day`,`start_time`,`end_time`)
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `time_slots`
--

LOCK TABLES `time_slots` WRITE;
/*!40000 ALTER TABLE `time_slots` DISABLE KEYS */;
INSERT INTO `time_slots` VALUES (25,'FRIDAY','11:30:00','10:30:00'),(26,'FRIDAY','12:30:00','11:30:00'),(27,'FRIDAY','13:30:00','12:30:00'),(28,'FRIDAY','15:30:00','14:30:00'),(29,'FRIDAY','16:30:00','15:30:00'),(30,'FRIDAY','17:30:00','16:30:00'),(1,'MONDAY','11:30:00','10:30:00'),(2,'MONDAY','12:30:00','11:30:00'),(3,'MONDAY','13:30:00','12:30:00'),(4,'MONDAY','15:30:00','14:30:00'),(5,'MONDAY','16:30:00','15:30:00'),(6,'MONDAY','17:30:00','16:30:00'),(19,'THURSDAY','11:30:00','10:30:00'),(20,'THURSDAY','12:30:00','11:30:00'),(21,'THURSDAY','13:30:00','12:30:00'),(22,'THURSDAY','15:30:00','14:30:00'),(23,'THURSDAY','16:30:00','15:30:00'),(24,'THURSDAY','17:30:00','16:30:00'),(7,'TUESDAY','11:30:00','10:30:00'),(8,'TUESDAY','12:30:00','11:30:00'),(9,'TUESDAY','13:30:00','12:30:00'),(10,'TUESDAY','15:30:00','14:30:00'),(11,'TUESDAY','16:30:00','15:30:00'),(12,'TUESDAY','17:30:00','16:30:00'),(13,'WEDNESDAY','11:30:00','10:30:00'),(14,'WEDNESDAY','12:30:00','11:30:00'),(15,'WEDNESDAY','13:30:00','12:30:00'),(16,'WEDNESDAY','15:30:00','14:30:00'),(17,'WEDNESDAY','16:30:00','15:30:00'),(18,'WEDNESDAY','17:30:00','16:30:00');
/*!40000 ALTER TABLE `time_slots` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `timetables`
--

DROP TABLE IF EXISTS `timetables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `timetables` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `batch_id` bigint NOT NULL,
  `subject_id` bigint NOT NULL,
  `teacher_id` bigint NOT NULL,
  `time_slot_id` bigint NOT NULL,
  `room_id` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_timetable_batch_slot` (`batch_id`,`time_slot_id`),
  UNIQUE KEY `uk_timetable_teacher_slot` (`teacher_id`,`time_slot_id`),
  UNIQUE KEY `uk_timetable_room_slot` (`room_id`,`time_slot_id`),
  KEY `fk_timetable_subject` (`subject_id`),
  KEY `fk_timetable_time_slot` (`time_slot_id`),
  CONSTRAINT `fk_timetable_batch` FOREIGN KEY (`batch_id`) REFERENCES `batches` (`id`),
  CONSTRAINT `fk_timetable_room` FOREIGN KEY (`room_id`) REFERENCES `rooms` (`id`),
  CONSTRAINT `fk_timetable_subject` FOREIGN KEY (`subject_id`) REFERENCES `subjects` (`id`),
  CONSTRAINT `fk_timetable_teacher` FOREIGN KEY (`teacher_id`) REFERENCES `teachers` (`id`),
  CONSTRAINT `fk_timetable_time_slot` FOREIGN KEY (`time_slot_id`) REFERENCES `time_slots` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=431 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `timetables`
--

-- LOCK TABLES `timetables` WRITE;
-- /*!40000 ALTER TABLE `timetables` DISABLE KEYS */;
-- INSERT INTO `timetables` VALUES (301,3,11,10,1,1),(302,2,11,10,2,2),(303,2,11,10,7,2),(304,1,11,10,3,3),(305,3,11,10,8,1),(306,2,11,10,13,12),(307,2,11,10,14,12),(308,2,11,10,15,12),(309,1,11,10,10,13),(310,1,11,10,11,13),(311,1,11,10,12,13),(312,1,11,10,16,3),(313,3,11,10,19,12),(314,3,11,10,20,12),(315,3,11,10,21,12),(316,1,14,6,19,3),(317,2,14,13,19,2),(318,3,14,6,13,1),(319,3,14,6,25,1),(320,3,14,6,2,1),(321,2,14,13,25,2),(322,2,15,29,4,13),(323,2,15,29,5,13),(324,2,15,29,6,13),(325,1,14,6,26,3),(326,1,15,31,4,12),(327,1,15,31,5,12),(328,1,15,31,6,12),(329,3,15,29,10,12),(330,3,15,29,11,12),(331,3,15,29,12,12),(332,2,14,13,8,2),(333,1,14,6,15,3),(334,2,6,19,20,2),(335,1,6,20,20,3),(336,1,6,20,28,9),(337,1,6,20,29,9),(338,1,6,20,30,9),(339,2,6,19,10,9),(340,2,6,19,11,9),(341,2,6,19,12,9),(342,1,6,20,14,3),(343,2,6,19,26,2),(344,1,6,20,9,3),(345,2,6,19,16,2),(346,3,5,22,14,1),(347,3,5,22,26,1),(348,3,5,22,4,8),(349,3,5,22,5,8),(350,3,5,22,6,8),(351,3,5,22,22,1),(352,2,2,14,21,2),(353,3,2,15,15,1),(354,2,2,14,27,2),(355,1,2,16,21,3),(356,1,2,16,13,3),(357,3,2,15,27,1),(358,3,2,15,9,1),(359,2,2,14,3,2),(360,3,2,15,23,1),(361,1,2,16,2,3),(362,1,2,16,8,3),(363,2,2,14,17,2),(364,3,10,1,16,1),(365,1,10,2,22,3),(366,1,10,2,27,3),(367,2,10,3,22,2),(368,1,10,2,17,13),(369,1,10,2,18,13),(370,1,10,2,1,3),(371,2,10,3,28,13),(372,2,10,3,29,13),(373,3,10,1,3,1),(374,3,10,1,28,12),(375,3,10,1,29,12),(376,2,10,3,1,2),(377,2,10,3,9,2),(378,3,10,1,7,1),(405,4,24,1,1,13),(406,4,24,1,2,13),(407,4,24,1,9,4),(408,4,28,10,22,13),(409,4,28,10,23,13),(410,4,28,10,17,12),(411,4,28,10,18,12),(412,4,25,13,26,13),(413,4,25,13,27,13),(414,4,23,4,7,12),(415,4,23,4,8,12),(416,4,24,1,21,4),(417,4,24,1,25,4),(418,4,25,13,3,4),(419,4,26,33,16,4),(420,4,25,13,10,4),(421,4,25,13,15,4),(422,4,26,33,4,4),(423,4,26,33,20,4),(424,4,26,33,28,4),(425,4,27,8,5,4),(426,4,23,4,14,4),(427,4,23,4,19,4),(428,4,23,4,29,4),(429,4,27,8,11,4),(430,4,27,8,13,4);
-- /*!40000 ALTER TABLE `timetables` ENABLE KEYS */;
-- UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `password` varchar(255) DEFAULT NULL,
  `role` enum('ADMIN','STUDENT','TEACHER') NOT NULL,
  `username` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKr43af9ap4edm43mmtq01oddj6` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES (1,'$2a$10$/1sFswXidYaaxeaqhvRAaO4PiVLPde1iwRmvSnhE/ngn2BEDZWNme','STUDENT','admin12');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-04-08 12:13:50
