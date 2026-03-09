-- Dedup-only script: skips merge steps, runs dedup directly
-- Usage: psql -U postgres -d postcode_lookup -f dedup-only.sql

\echo 'Starting UPRN dedup - reassigning FKs...'

UPDATE price_paid et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE uprn IS NOT NULL) ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'UPRN FK: price_paid done'

UPDATE companies et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE uprn IS NOT NULL) ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'UPRN FK: companies done'

UPDATE food_ratings et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE uprn IS NOT NULL) ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'UPRN FK: food_ratings done'

UPDATE voa_ratings et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE uprn IS NOT NULL) ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'UPRN FK: voa_ratings done'

\echo 'Deleting UPRN duplicates...'
DELETE FROM addresses a USING (SELECT id, ROW_NUMBER() OVER (PARTITION BY uprn ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE uprn IS NOT NULL) ranked WHERE a.id = ranked.id AND ranked.rn > 1;
\echo 'UPRN dedup complete'

\echo 'Starting Text dedup - reassigning FKs...'

UPDATE price_paid et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE postcode_norm IS NOT NULL AND street IS NOT NULL AND street != '') ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'Text FK: price_paid done'

UPDATE companies et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE postcode_norm IS NOT NULL AND street IS NOT NULL AND street != '') ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'Text FK: companies done'

UPDATE food_ratings et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE postcode_norm IS NOT NULL AND street IS NOT NULL AND street != '') ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'Text FK: food_ratings done'

UPDATE voa_ratings et SET address_id = ranked.keeper_id FROM (SELECT id, FIRST_VALUE(id) OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS keeper_id, ROW_NUMBER() OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE postcode_norm IS NOT NULL AND street IS NOT NULL AND street != '') ranked WHERE et.address_id = ranked.id AND ranked.rn > 1 AND ranked.keeper_id != ranked.id;
\echo 'Text FK: voa_ratings done'

\echo 'Deleting text duplicates...'
DELETE FROM addresses a USING (SELECT id, ROW_NUMBER() OVER (PARTITION BY postcode_norm, UPPER(COALESCE(street,'')), UPPER(COALESCE(house_number,'')), UPPER(COALESCE(flat,'')), UPPER(COALESCE(house_name,'')), UPPER(COALESCE(city,'')) ORDER BY confidence DESC NULLS LAST, id ASC) AS rn FROM addresses WHERE postcode_norm IS NOT NULL AND street IS NOT NULL AND street != '') ranked WHERE a.id = ranked.id AND ranked.rn > 1;
\echo 'Text dedup complete'

\echo 'ALL DONE - Dedup finished!'
