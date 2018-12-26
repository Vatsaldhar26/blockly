--
-- Name: _final_median(anyarray); Type: FUNCTION; Schema: public; Owner: blueoptima
--

CREATE FUNCTION _final_median(anyarray) RETURNS double precision
    LANGUAGE sql IMMUTABLE
    AS $_$ 
	WITH q AS (
		SELECT val
		FROM unnest($1) val
		WHERE VAL IS NOT NULL
		ORDER BY 1
	), cnt AS (
		SELECT COUNT(*) AS c FROM q
	)
	SELECT AVG(val)::float8
	FROM (
		SELECT val FROM q
		LIMIT  2 - MOD((SELECT c FROM cnt), 2)
		OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)) q2;
	$_$;


ALTER FUNCTION public._final_median(anyarray) OWNER TO blueoptima;

--
-- Name: add_extraction_requests(integer[], date, date); Type: FUNCTION; Schema: public; Owner: blueoptima
--

CREATE FUNCTION add_extraction_requests(infra_ids integer[], start_date date, end_date date) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
		infra_id	INTEGER;
		req_count	INTEGER;
		req_start DATE;
		req_end   DATE;
		
		req_id TEXT;
	BEGIN
		req_count = 0;
		FOR infra_id IN SELECT unnest(infra_ids)
		LOOP
			req_start = start_date;
			WHILE req_start < end_date
			LOOP
				req_end = req_start + integer '30';
				if (req_end > end_date) THEN
					req_end = end_date;
				END IF;
				-- RAISE NOTICE 'Infra ID: % => Req Duration: % to % ', infra_id, req_start, req_end;

				req_id = uuid_generate_v4();
				INSERT INTO po_import_request (id_import_request, id_infra_instan, tx_import_status, ts_start_date, ts_end_date, ts_create_time, nu_retry_count, id_acquisition_type)
				VALUES (req_id, infra_id, 'Request Queued on Server', req_start, req_end, now(), 0, 3);
				INSERT INTO po_request_queue (id_import_request, id_infra_instan, ts_start_date, ts_end_date, nu_request_type)
				VALUES (req_id, infra_id, req_start, req_end, 2);
				
				req_start = req_end;
				req_count = req_count + 1;
			END LOOP;
		END LOOP;
		RETURN req_count;
	END;
$$;


ALTER FUNCTION public.add_extraction_requests(infra_ids integer[], start_date date, end_date date) OWNER TO blueoptima;