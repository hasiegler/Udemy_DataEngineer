-- Run this once in Supabase SQL Editor (Dashboard → SQL Editor).
-- Creates schemas for Airflow metadata and Celery results so they don't clash with your ELT data in public.

CREATE SCHEMA IF NOT EXISTS airflow_metadata;
CREATE SCHEMA IF NOT EXISTS celery_results;
