--
-- PostgreSQL database dump
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--
-- Name: authors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.authors (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    name text NOT NULL,
    birthdate date
);


--
-- Name: books; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.books (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    title text NOT NULL,
    isbn text NOT NULL,
    publisher text DEFAULT ''::text NOT NULL,
    author_id uuid,
    uuid_test uuid
);


--
-- Name: distributors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.distributors (
    id integer NOT NULL,
    company_name character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    last_shipment timestamp without time zone
);


--
-- Name: purchasers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.purchasers (
    id integer NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    email character varying(255)
);

--
-- Data for Name: authors; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.authors (id, created_at, updated_at, name, birthdate) FROM stdin;
bcb8845d-e0f8-429a-b032-fdc89742b192	2018-07-30 17:00:00-07	2018-07-30 17:00:00-07	Frobbert Gumpledunkins	2018-07-30
a0d0130c-9285-41b9-b64a-8bce8f0a8f7b	2018-07-30 17:00:00-07	2018-07-30 17:00:00-07	Munkle Wilponkerlicious	2018-07-30
2e4c70c1-8ed7-4a70-a8a8-5cab246d456b	2018-10-12 13:48:02.258905-07	2018-10-12 13:48:02.258905-07	Levi D Junkert	\N
\.


--
-- Data for Name: books; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.books (id, created_at, updated_at, title, isbn, publisher, author_id, uuid_test) FROM stdin;
a14d2554-22d3-47d6-bbf7-17207ff1e8dc	2018-07-30 17:00:00-07	2018-07-30 17:00:00-07	Dummies for Dummies	102	ShamCo	a0d0130c-9285-41b9-b64a-8bce8f0a8f7b	\N
f8e3f579-3deb-4ded-b927-19b5b9d256cf	2018-07-29 17:00:00-07	2018-07-29 17:00:00-07	Book Titles in 5 Words 02 	333	Rip Toft Books	a0d0130c-9285-41b9-b64a-8bce8f0a8f7b	\N
fac2b15c-7c99-4447-8130-285b5d3bf7f1	2018-07-29 17:00:00-07	2018-07-29 17:00:00-07	How to How to	4	ShamCo	bcb8845d-e0f8-429a-b032-fdc89742b192	\N
3193526e-3fe6-4ab7-8080-1c6280817118	2018-10-12 13:55:29.886874-07	2018-10-12 13:55:29.886874-07	I Dont Need No Stinkin Tests, Yo!	1	We Steal Books Inc.	2e4c70c1-8ed7-4a70-a8a8-5cab246d456b	\N
\.


--
-- Data for Name: distributors; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.distributors (id, company_name, email, last_shipment) FROM stdin;
1	Books-R-Us	books-r-us@example.com	2018-10-17 12:08:39.390863
2	You Need Books, We Got Books Inc.	pawnee@example.com	2018-10-17 12:08:39.403764
3	Banned Books LLC	bannd-books@example.com	2018-10-17 12:08:40.264615
\.


--
-- Data for Name: purchasers; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.purchasers (id, first_name, last_name, email) FROM stdin;
1	Norman	Smith	norman@example.com
2	Janet	Borders	borders@example.com
3	Stephanie	Dent	steph@example.com
4	Columbo	Gallardo	columbo@example.com
\.


CREATE OR REPLACE FUNCTION create_distributor(com varchar, eml varchar) RETURNS VOID AS $$
BEGIN
    INSERT INTO public.distributors(id, company_name, email, last_shipment) VALUES (4, com, eml, '1970/01/01 00:00:00');
END
$$ LANGUAGE 'plpgsql' STABLE;

--
-- PostgreSQL database dump complete
--

