-- Source: https://supabase.com/docs/guides/database/postgres/timeouts#global-level
alter database postgres set statement_timeout TO '60min';
