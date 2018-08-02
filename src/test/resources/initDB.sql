--
-- PostgreSQL database dump
--

--
-- Name: configuration; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.configuration (
    readgloballimit bigint NOT NULL,
    writegloballimit bigint NOT NULL,
    readsessionlimit bigint NOT NULL,
    writesessionlimit bigint NOT NULL,
    delaylimit bigint NOT NULL,
    updatedinfo integer NOT NULL,
    hostid character varying(8096) NOT NULL
);

--
-- Name: hostconfig; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.hostconfig (
    business text NOT NULL,
    roles text NOT NULL,
    aliases text NOT NULL,
    others text NOT NULL,
    updatedinfo integer NOT NULL,
    hostid character varying(8096) NOT NULL
);

--
-- Name: hosts; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.hosts (
    address character varying(8096) NOT NULL,
    port integer NOT NULL,
    isssl boolean NOT NULL,
    hostkey bytea NOT NULL,
    adminrole boolean NOT NULL,
    isclient boolean NOT NULL,
    isactive boolean NOT NULL,
    isproxified boolean NOT NULL,
    updatedinfo integer NOT NULL,
    hostid character varying(8096) NOT NULL
);

--
-- Name: multiplemonitor; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.multiplemonitor (
    countconfig integer NOT NULL,
    counthost integer NOT NULL,
    countrule integer NOT NULL,
    hostid character varying(8096) NOT NULL
);

--
-- Name: rules; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.rules (
    hostids text,
    modetrans integer,
    recvpath character varying(8096),
    sendpath character varying(8096),
    archivepath character varying(8096),
    workpath character varying(8096),
    rpretasks text,
    rposttasks text,
    rerrortasks text,
    spretasks text,
    sposttasks text,
    serrortasks text,
    updatedinfo integer,
    idrule character varying(8096) NOT NULL
);

--
-- Name: runner; Type: TABLE; Schema: public; Owner: waarp
--

CREATE TABLE public.runner (
    globalstep integer NOT NULL,
    globallaststep integer NOT NULL,
    step integer NOT NULL,
    rank integer NOT NULL,
    stepstatus character(3) NOT NULL,
    retrievemode boolean NOT NULL,
    filename character varying(8096) NOT NULL,
    ismoved boolean NOT NULL,
    idrule character varying(8096) NOT NULL,
    blocksz integer NOT NULL,
    originalname character varying(8096) NOT NULL,
    fileinfo text NOT NULL,
    transferinfo text NOT NULL,
    modetrans integer NOT NULL,
    starttrans timestamp without time zone NOT NULL,
    stoptrans timestamp without time zone NOT NULL,
    infostatus character(3) NOT NULL,
    updatedinfo integer NOT NULL,
    ownerreq character varying(8096) NOT NULL,
    requester character varying(8096) NOT NULL,
    requested character varying(8096) NOT NULL,
    specialid bigint NOT NULL
);

--
-- Name: runseq; Type: SEQUENCE; Schema: public; Owner: waarp
--

CREATE SEQUENCE public.runseq
    START WITH -9223372036854775807
    INCREMENT BY 1
    MINVALUE -9223372036854775807
    NO MAXVALUE
    CACHE 1;

--
-- Data for Name: configuration; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.configuration VALUES (1, 2, 3, 4, 5, 42, 'server1');
INSERT INTO public.configuration VALUES (2, 3, 2, 2, 2, 81, 'server2');
INSERT INTO public.configuration VALUES (5, 6, 3, 4, 3, 1, 'server3');


--
-- Data for Name: hostconfig; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.hostconfig VALUES ('joyaux', 'marchand', 'le borgne', 'misc', 17, 'server1');
INSERT INTO public.hostconfig VALUES ('ba', '', '', '<root><version>3.0.9</version></root>', 0, 'server2');
INSERT INTO public.hostconfig VALUES ('ba', '', '', '<root><version>3.0.9</version></root>', 0, 'server3');
INSERT INTO public.hostconfig VALUES ('', '', '', '<root><version>3.0.9</version></root>', 0, 'server4');
INSERT INTO public.hostconfig VALUES ('', '', '', '<root><version>3.0.9</version></root>', 0, 'server5');


--
-- Data for Name: hosts; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.hosts VALUES ('127.0.0.1', 6666, true, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, true, false, true, 42, 'server1');
INSERT INTO public.hosts VALUES ('127.0.0.1', 6667, true, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, false, true, false, 0, 'server1-ssl');
INSERT INTO public.hosts VALUES ('127.0.0.3', 6676, false, '\x303465626439323336346235616136306332396630346461353132616361346265303639646336633661383432653235', false, false, true, false, 0, 'server2');


--
-- Data for Name: multiplemonitor; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.multiplemonitor VALUES (11, 18, 29, 'server1');
INSERT INTO public.multiplemonitor VALUES (1, 2, 3, 'server2');
INSERT INTO public.multiplemonitor VALUES (0, 0, 0, 'server3');
INSERT INTO public.multiplemonitor VALUES (0, 0, 0, 'server4');


--
-- Data for Name: rules; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.rules VALUES ('<hostids></hostids>', 1, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', 0, 'default');
INSERT INTO public.rules VALUES ('<hostids><hostid>blabla</hostid><hostid>blabla2</hostid><hostid>blabla3</hostid></hostids>', 1, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task><task><type>test</type><path>aa</path><delay>1</delay></task></tasks>', '<tasks></tasks>', '<tasks></tasks>', 42, 'dummy');
INSERT INTO public.rules VALUES ('<hostids></hostids>', 3, '/in', '/out', '/arch', '/work', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', '<tasks></tasks>', 0, 'dummy2');

--
-- Data for Name: runner; Type: TABLE DATA; Schema: public; Owner: waarp
--

INSERT INTO public.runner VALUES (5, 0, 0, 0, 'C  ', true, 'data/server1/log/client.log', false, 'default', 65536, 'data/server1/log/client.log', 'noinfo', '{"ORIGINALSIZE":18391}', 1, '2018-06-27 14:31:37.738', '2018-06-27 14:31:58.042', 'C  ', 4, 'server1', 'server1', 'server2', -9223372036854775807);
INSERT INTO public.runner VALUES (5, 0, 0, 0, 'C  ', true, 'data/server1/log/client.log', false, 'default', 65536, 'data/server1/log/client.log', 'noinfo', '{"ORIGINALSIZE":52587}', 1, '2018-06-27 14:36:00.116', '2018-06-27 14:36:20.374', 'C  ', 4, 'server1', 'server1', 'server2', -9223372036854775806);
INSERT INTO public.runner VALUES (5, 0, 0, 0, 'C  ', true, 'tintin', false, 'tintin', 65536, 'tintin', 'noinfo', '{"ORIGINALSIZE":-1}', 1, '2018-06-27 14:39:01.28', '2018-06-27 14:39:21.518', 'C  ', 4, 'server1', 'server1', 'server2', -9223372036854775805);
INSERT INTO public.runner VALUES (5, 0, 0, 0, 'C  ', true, 'tintin', false, 'default', 65536, 'tintin', 'noinfo', '{"ORIGINALSIZE":-1}', 1, '2018-06-27 14:39:01.28', '2018-06-27 14:39:21.518', 'C  ', 4, 'server1', 'server1', 'server2', 0);


--
-- Name: runseq; Type: SEQUENCE SET; Schema: public; Owner: waarp
--

SELECT pg_catalog.setval('public.runseq', -9223372036854775805, true);


--
-- Name: configuration configuration_pkey; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.configuration
    ADD CONSTRAINT configuration_pkey PRIMARY KEY (hostid);


--
-- Name: hostconfig hostconfig_pkey; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.hostconfig
    ADD CONSTRAINT hostconfig_pkey PRIMARY KEY (hostid);


--
-- Name: hosts hosts_pkey; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.hosts
    ADD CONSTRAINT hosts_pkey PRIMARY KEY (hostid);


--
-- Name: multiplemonitor multiplemonitor_pkey; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.multiplemonitor
    ADD CONSTRAINT multiplemonitor_pkey PRIMARY KEY (hostid);


--
-- Name: rules rules_pkey; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.rules
    ADD CONSTRAINT rules_pkey PRIMARY KEY (idrule);


--
-- Name: runner runner_pk; Type: CONSTRAINT; Schema: public; Owner: waarp
--

ALTER TABLE ONLY public.runner
    ADD CONSTRAINT runner_pk PRIMARY KEY (ownerreq, requester, requested, specialid);


--
-- Name: idx_runner; Type: INDEX; Schema: public; Owner: waarp
--

CREATE INDEX idx_runner ON public.runner USING btree (starttrans, ownerreq, stepstatus, updatedinfo, globalstep, infostatus, specialid);


--
-- PostgreSQL database dump complete
--

