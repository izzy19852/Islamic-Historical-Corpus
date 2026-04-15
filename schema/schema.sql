--
-- PostgreSQL database dump
--

\restrict r6lRg1hTcDRrFMkaruIzOEJcRCM2SkGdtThNv2Eu3VYEnhwRmlr64fZPHTy4501

-- Dumped from database version 15.17 (Debian 15.17-1.pgdg12+1)
-- Dumped by pg_dump version 15.17 (Debian 15.17-1.pgdg12+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_keys (
    id integer NOT NULL,
    key_hash text NOT NULL,
    name text NOT NULL,
    tier text DEFAULT 'free'::text,
    query_count integer DEFAULT 0,
    month_count integer DEFAULT 0,
    month_reset date DEFAULT CURRENT_DATE,
    active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: api_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.api_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: api_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.api_keys_id_seq OWNED BY public.api_keys.id;


--
-- Name: chunk_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chunk_metadata (
    id integer NOT NULL,
    chunk_id integer,
    figure_ids integer[],
    event_id integer,
    account_type text,
    chain_strength text,
    conflict_flag boolean DEFAULT false,
    conflict_note text,
    noise_flag boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT chunk_metadata_account_type_check CHECK ((account_type = ANY (ARRAY['eyewitness'::text, 'transmitted'::text, 'later_compilation'::text, 'commentary'::text]))),
    CONSTRAINT chunk_metadata_chain_strength_check CHECK ((chain_strength = ANY (ARRAY['sahih'::text, 'hasan'::text, 'daif'::text, 'unknown'::text, 'scholarly'::text])))
);


--
-- Name: chunk_metadata_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.chunk_metadata_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: chunk_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.chunk_metadata_id_seq OWNED BY public.chunk_metadata.id;


--
-- Name: documents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.documents (
    id integer NOT NULL,
    content text NOT NULL,
    embedding public.vector(1024),
    source text NOT NULL,
    source_type text NOT NULL,
    era text,
    figures text[],
    chunk_index integer,
    word_count integer,
    created_at timestamp without time zone DEFAULT now(),
    metadata jsonb DEFAULT '{}'::jsonb
);


--
-- Name: documents_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.documents_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: documents_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.documents_id_seq OWNED BY public.documents.id;


--
-- Name: events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.events (
    id integer NOT NULL,
    name text NOT NULL,
    name_variants text[] DEFAULT '{}'::text[],
    date_ce text,
    date_ah text,
    location text,
    era text,
    figure_ids integer[],
    significance text,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id;


--
-- Name: figure_lineage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.figure_lineage (
    id integer NOT NULL,
    figure_id integer NOT NULL,
    related_id integer,
    related_name text,
    lineage_type text NOT NULL,
    direction text NOT NULL,
    divergence text,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT figure_lineage_direction_check CHECK ((direction = ANY (ARRAY['ancestor'::text, 'descendant'::text]))),
    CONSTRAINT figure_lineage_divergence_check CHECK ((divergence = ANY (ARRAY['SURPASSED'::text, 'BETRAYED'::text, 'COMPLETED'::text, 'CORRUPTED'::text, 'ABANDONED'::text, 'MARTYRED'::text]))),
    CONSTRAINT figure_lineage_lineage_type_check CHECK ((lineage_type = ANY (ARRAY['BIOLOGICAL'::text, 'POLITICAL_HEIR'::text, 'MILITARY_PATRON'::text, 'INTELLECTUAL'::text, 'SUFI_SILSILA'::text])))
);


--
-- Name: figure_lineage_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.figure_lineage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: figure_lineage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.figure_lineage_id_seq OWNED BY public.figure_lineage.id;


--
-- Name: figure_relationships; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.figure_relationships (
    id integer NOT NULL,
    figure_a_id integer NOT NULL,
    figure_b_id integer NOT NULL,
    relationship text NOT NULL,
    description text,
    resolution text,
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT figure_relationships_relationship_check CHECK ((relationship = ANY (ARRAY['ALLY'::text, 'ANTAGONIST'::text, 'RIVAL'::text, 'MUTUAL_RESPECT'::text, 'IDEOLOGICAL_OPPONENT'::text, 'PARALLEL'::text, 'POLITICAL_OPPONENT'::text]))),
    CONSTRAINT figure_relationships_resolution_check CHECK ((resolution = ANY (ARRAY['RECONCILED'::text, 'UNRESOLVED'::text, 'VICTORY_A'::text, 'VICTORY_B'::text, 'MUTUAL_DESTRUCTION'::text, 'TRANSCENDED'::text, 'DEATH_ENDED_IT'::text])))
);


--
-- Name: figure_relationships_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.figure_relationships_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: figure_relationships_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.figure_relationships_id_seq OWNED BY public.figure_relationships.id;


--
-- Name: figures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.figures (
    id integer NOT NULL,
    name text NOT NULL,
    name_variants text[] DEFAULT '{}'::text[],
    sensitivity_tier character(1) NOT NULL,
    era text[] DEFAULT '{}'::text[],
    series text[] DEFAULT '{}'::text[],
    birth_death text,
    dramatic_question text,
    primary_sources text[] DEFAULT '{}'::text[],
    generation text,
    tabaqat_volume integer,
    sahabi_categories text[] DEFAULT '{}'::text[],
    bayah_pledges text[] DEFAULT '{}'::text[],
    known_for text,
    primary_hadith_count integer DEFAULT 0,
    death_circumstance text,
    created_at timestamp without time zone DEFAULT now(),
    ethnicity text,
    physical_description text,
    appearance_source text,
    kling_appearance text,
    appearance_confidence text,
    dress_description text,
    weapons_description text,
    armor_description text,
    fighting_style text,
    kling_costume text,
    kling_full_character text,
    CONSTRAINT figures_death_circumstance_check CHECK ((death_circumstance = ANY (ARRAY['battle'::text, 'plague'::text, 'martyrdom'::text, 'natural'::text, 'executed'::text, 'assassinated'::text, 'unknown'::text]))),
    CONSTRAINT figures_generation_check CHECK ((generation = ANY (ARRAY['sahabi'::text, 'tabi_i'::text, 'tabi_al_tabi_in'::text, 'later'::text]))),
    CONSTRAINT figures_sensitivity_tier_check CHECK ((sensitivity_tier = ANY (ARRAY['S'::bpchar, 'A'::bpchar, 'B'::bpchar, 'C'::bpchar])))
);


--
-- Name: figures_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.figures_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: figures_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.figures_id_seq OWNED BY public.figures.id;


--
-- Name: scholarly_debates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scholarly_debates (
    id integer NOT NULL,
    topic text NOT NULL,
    event_id integer,
    figure_id integer,
    position_a text,
    position_b text,
    key_scholars text[],
    script_instruction text,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: scholarly_debates_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scholarly_debates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scholarly_debates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scholarly_debates_id_seq OWNED BY public.scholarly_debates.id;


--
-- Name: source_relationships; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_relationships (
    id integer NOT NULL,
    source_a text NOT NULL,
    source_b text NOT NULL,
    event_id integer,
    relationship text NOT NULL,
    conflict_note text,
    reliability_note text,
    scholarly_consensus text,
    created_at timestamp without time zone DEFAULT now(),
    topic text,
    position_a text,
    position_b text,
    reconciliation text,
    script_instruction text,
    dramatic_weight text,
    CONSTRAINT source_relationships_dramatic_weight_check CHECK ((dramatic_weight = ANY (ARRAY['LOW'::text, 'MEDIUM'::text, 'HIGH'::text, 'CRITICAL'::text]))),
    CONSTRAINT source_relationships_relationship_check CHECK ((relationship = ANY (ARRAY['CORROBORATES'::text, 'CONTRADICTS'::text, 'SUPPLEMENTS'::text, 'CHALLENGES'::text, 'EARLIER_ACCOUNT'::text, 'LATER_COMPILATION'::text])))
);


--
-- Name: source_relationships_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.source_relationships_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: source_relationships_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.source_relationships_id_seq OWNED BY public.source_relationships.id;


--
-- Name: api_keys id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys ALTER COLUMN id SET DEFAULT nextval('public.api_keys_id_seq'::regclass);


--
-- Name: chunk_metadata id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chunk_metadata ALTER COLUMN id SET DEFAULT nextval('public.chunk_metadata_id_seq'::regclass);


--
-- Name: documents id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.documents ALTER COLUMN id SET DEFAULT nextval('public.documents_id_seq'::regclass);


--
-- Name: events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);


--
-- Name: figure_lineage id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_lineage ALTER COLUMN id SET DEFAULT nextval('public.figure_lineage_id_seq'::regclass);


--
-- Name: figure_relationships id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_relationships ALTER COLUMN id SET DEFAULT nextval('public.figure_relationships_id_seq'::regclass);


--
-- Name: figures id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figures ALTER COLUMN id SET DEFAULT nextval('public.figures_id_seq'::regclass);


--
-- Name: scholarly_debates id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scholarly_debates ALTER COLUMN id SET DEFAULT nextval('public.scholarly_debates_id_seq'::regclass);


--
-- Name: source_relationships id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_relationships ALTER COLUMN id SET DEFAULT nextval('public.source_relationships_id_seq'::regclass);


--
-- Name: api_keys api_keys_key_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_key_hash_key UNIQUE (key_hash);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: chunk_metadata chunk_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chunk_metadata
    ADD CONSTRAINT chunk_metadata_pkey PRIMARY KEY (id);


--
-- Name: documents documents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.documents
    ADD CONSTRAINT documents_pkey PRIMARY KEY (id);


--
-- Name: events events_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_name_key UNIQUE (name);


--
-- Name: events events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id);


--
-- Name: figure_lineage figure_lineage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_lineage
    ADD CONSTRAINT figure_lineage_pkey PRIMARY KEY (id);


--
-- Name: figure_relationships figure_relationships_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_relationships
    ADD CONSTRAINT figure_relationships_pkey PRIMARY KEY (id);


--
-- Name: figures figures_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figures
    ADD CONSTRAINT figures_name_key UNIQUE (name);


--
-- Name: figures figures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figures
    ADD CONSTRAINT figures_pkey PRIMARY KEY (id);


--
-- Name: scholarly_debates scholarly_debates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scholarly_debates
    ADD CONSTRAINT scholarly_debates_pkey PRIMARY KEY (id);


--
-- Name: source_relationships source_relationships_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_relationships
    ADD CONSTRAINT source_relationships_pkey PRIMARY KEY (id);


--
-- Name: idx_chunk_metadata_chunk_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chunk_metadata_chunk_id ON public.chunk_metadata USING btree (chunk_id);


--
-- Name: idx_chunk_metadata_event_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chunk_metadata_event_id ON public.chunk_metadata USING btree (event_id);


--
-- Name: idx_chunk_metadata_figure_ids; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chunk_metadata_figure_ids ON public.chunk_metadata USING gin (figure_ids);


--
-- Name: idx_documents_embedding; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_documents_embedding ON public.documents USING hnsw (embedding public.vector_cosine_ops);


--
-- Name: idx_documents_era; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_documents_era ON public.documents USING btree (era);


--
-- Name: idx_documents_figures; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_documents_figures ON public.documents USING gin (figures);


--
-- Name: idx_documents_source_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_documents_source_type ON public.documents USING btree (source_type);


--
-- Name: idx_events_era; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_era ON public.events USING btree (era);


--
-- Name: idx_events_figure_ids; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_figure_ids ON public.events USING gin (figure_ids);


--
-- Name: idx_figure_lineage_figure; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figure_lineage_figure ON public.figure_lineage USING btree (figure_id);


--
-- Name: idx_figure_relationships_a; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figure_relationships_a ON public.figure_relationships USING btree (figure_a_id);


--
-- Name: idx_figure_relationships_b; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figure_relationships_b ON public.figure_relationships USING btree (figure_b_id);


--
-- Name: idx_figures_era; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figures_era ON public.figures USING gin (era);


--
-- Name: idx_figures_generation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figures_generation ON public.figures USING btree (generation);


--
-- Name: idx_figures_tier; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_figures_tier ON public.figures USING btree (sensitivity_tier);


--
-- Name: idx_scholarly_debates_event; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scholarly_debates_event ON public.scholarly_debates USING btree (event_id);


--
-- Name: idx_scholarly_debates_figure; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scholarly_debates_figure ON public.scholarly_debates USING btree (figure_id);


--
-- Name: chunk_metadata chunk_metadata_chunk_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chunk_metadata
    ADD CONSTRAINT chunk_metadata_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.documents(id);


--
-- Name: chunk_metadata chunk_metadata_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chunk_metadata
    ADD CONSTRAINT chunk_metadata_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id);


--
-- Name: figure_lineage figure_lineage_figure_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_lineage
    ADD CONSTRAINT figure_lineage_figure_id_fkey FOREIGN KEY (figure_id) REFERENCES public.figures(id);


--
-- Name: figure_lineage figure_lineage_related_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_lineage
    ADD CONSTRAINT figure_lineage_related_id_fkey FOREIGN KEY (related_id) REFERENCES public.figures(id);


--
-- Name: figure_relationships figure_relationships_figure_a_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_relationships
    ADD CONSTRAINT figure_relationships_figure_a_id_fkey FOREIGN KEY (figure_a_id) REFERENCES public.figures(id);


--
-- Name: figure_relationships figure_relationships_figure_b_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.figure_relationships
    ADD CONSTRAINT figure_relationships_figure_b_id_fkey FOREIGN KEY (figure_b_id) REFERENCES public.figures(id);


--
-- Name: scholarly_debates scholarly_debates_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scholarly_debates
    ADD CONSTRAINT scholarly_debates_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id);


--
-- Name: scholarly_debates scholarly_debates_figure_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scholarly_debates
    ADD CONSTRAINT scholarly_debates_figure_id_fkey FOREIGN KEY (figure_id) REFERENCES public.figures(id);


--
-- Name: source_relationships source_relationships_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_relationships
    ADD CONSTRAINT source_relationships_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(id);


--
-- PostgreSQL database dump complete
--

\unrestrict r6lRg1hTcDRrFMkaruIzOEJcRCM2SkGdtThNv2Eu3VYEnhwRmlr64fZPHTy4501

