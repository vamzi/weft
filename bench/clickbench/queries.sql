-- ClickBench queries (vendored from ClickHouse/ClickBench main/clickhouse/queries.sql).
-- The full 43 are pulled in by `install`; a few are inlined here as anchors / smoke tests.
-- Numbering below is 0-based to match upstream (the plan's "Q1/Q7/Q24/Q34/Q35" are 1-based).
SELECT COUNT(*) FROM hits;                                                            -- 0 scan/metadata
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;                                     -- 1 filter
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;                    -- 2 scan-agg
SELECT MIN(EventDate), MAX(EventDate) FROM hits;                                      -- 6 scan/metadata (plan "Q7")
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;            -- 33 high-card GROUP BY (plan "Q34")
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;      -- 34 high-card GROUP BY (plan "Q35")
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;   -- 23 sort/top-N (plan "Q24", Sail's slowest)
-- TODO(issue #3): vendor all 43 from upstream rather than this anchor subset.
