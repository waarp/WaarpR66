--
-- Data for Name: configuration; Type: TABLE DATA;
--

INSERT INTO CONFIGURATION VALUES (1, 2, 3, 4, 5, 42, 'server1');
INSERT INTO CONFIGURATION VALUES (2, 3, 2, 2, 2, 81, 'server2');
INSERT INTO CONFIGURATION VALUES (5, 6, 3, 4, 3, 1, 'server3');


--
-- Data for Name: hostconfig; Type: TABLE DATA;
--

INSERT INTO HOSTCONFIG VALUES ('joyaux', 'marchand', 'le borgne', 'misc', 17, 'server1');
INSERT INTO HOSTCONFIG VALUES ('ba', '', '', '<root><version>3.0.9</version></root>', 0, 'server2');
INSERT INTO HOSTCONFIG VALUES ('ba', '', '', '<root><version>3.0.9</version></root>', 0, 'server3');
INSERT INTO HOSTCONFIG VALUES ('', '', '', '<root><version>3.0.9</version></root>', 0, 'server4');
INSERT INTO HOSTCONFIG VALUES ('', '', '', '<root><version>3.0.9</version></root>', 0, 'server5');


--
-- Data for Name: hosts; Type: TABLE DATA;
--

INSERT INTO HOSTS VALUES ('127.0.0.1', 6666, true, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, true, false, true, 42, 'server1');
INSERT INTO HOSTS VALUES ('127.0.0.1', 6667, true, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, false, true, false, 0, 'server1-ssl');
INSERT INTO HOSTS VALUES ('127.0.0.3', 6676, false, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, false, true, false, 0, 'server2');


--
-- Data for Name: multiplemonitor; Type: TABLE DATA;
--

INSERT INTO MULTIPLEMONITOR VALUES (11, 18, 29, 'server1');
INSERT INTO MULTIPLEMONITOR VALUES (1, 2, 3, 'server2');
INSERT INTO MULTIPLEMONITOR VALUES (0, 0, 0, 'server3');
INSERT INTO MULTIPLEMONITOR VALUES (0, 0, 0, 'server4');


--
-- Data for Name: rules; Type: TABLE DATA;
--

INSERT INTO RULES VALUES ('<hostids></hostids>', 1, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', 0, 'default');
INSERT INTO RULES VALUES ('<hostids><hostid>blabla</hostid><hostid>blabla2</hostid><hostid>blabla3</hostid></hostids>', 1, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks></tasks>', '<tasks></tasks>', 42, 'dummy');
INSERT INTO RULES VALUES ('<hostids></hostids>', 3, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', 0, 'dummy2');

--
-- Data for Name: runner; Type: TABLE DATA;
--

INSERT INTO RUNNER VALUES (5, 0, 0, 0, 'C  ', true, 'data/server1/log/client.log', false, 'default', 65536, 'data/server1/log/client.log', 'noinfo', '{"ORIGINALSIZE":18391}', 1, '2018-06-27 14:31:37.738', '2018-06-27 14:31:58.042', 'C  ', 5, 'server1', 'server1', 'server2', -9223372036854775807);
INSERT INTO RUNNER VALUES (5, 0, 0, 0, 'C  ', true, 'data/server1/log/client.log', false, 'default', 65536, 'data/server1/log/client.log', 'noinfo', '{"ORIGINALSIZE":52587}', 1, '2018-06-20 14:36:00.116', '2018-06-20 14:36:20.374', 'C  ', 4, 'server1', 'server1', 'server2', -9223372036854775806);
INSERT INTO RUNNER VALUES (5, 0, 0, 0, 'C  ', true, 'tintin', false, 'tintin', 65536, 'tintin', 'noinfo', '{"ORIGINALSIZE":-1}', 1, '2018-06-22 14:39:01.28', '2018-06-22 14:39:21.518', 'C  ', 4, 'server1', 'server1', 'server2', -9223372036854775805);
INSERT INTO RUNNER VALUES (5, 0, 0, 0, 'C  ', true, 'tintin', false, 'default', 65536, 'tintin', 'noinfo', '{"ORIGINALSIZE":-1}', 1, '2018-06-24 14:39:01.28', '2018-06-24 14:39:21.518', 'C  ', 4, 'server1', 'server1', 'server2', 0);
