-- This query creates the cohort used for the callout project ("Discharge Devils")
-- The query requires the following materalized views/tables:
--  ventdurations - created by ventilation-durations.sql

drop materialized view IF EXISTS dd_cohort_all CASCADE;
create materialized view dd_cohort_all as
with ch as (
select ie.subject_id, ie.hadm_id, ie.icustay_id
, case
      when adm.deathtime is not null then 'Y'
      else 'N' end
    as hospital_expire_flag

-- ICU admission/discharge times
, ie.intime, ie.outtime

-- Hospital admission/discharge times
, adm.admittime, adm.dischtime

-- Patient death time

-- fixes ~10 typos in death date which likely don't even show up in the cohort
, case
    when adm.deathtime is not null
    then adm.dischtime
  else null
  end as deathtime

-- Patient's age on ICU admission
, round( (cast(ie.intime as date) - cast(pat.dob as date)) /365.242,4) as Age
, pat.gender
, adm.ethnicity

, case
    when adm.ADMISSION_TYPE = 'ELECTIVE' then 1
    else 0
  end as Elective

, adm.has_chartevents_data -- hospital admission with data

-- not a child
, case
    when round( (cast(ie.intime as date) - cast(pat.dob as date)) /365.242,4) > 15
      then 1
    else 0
  end as adult

-- An integer which is 1 for the first ICU stay of the hospitalization
-- Increases by 1 for each subsequent ICU admission
, row_number() over (partition by ie.subject_id, ie.hadm_id order by ie.intime) as icustay_num
from mimiciii.icustays ie
inner join mimiciii.admissions adm
 on ie.hadm_id = adm.hadm_id
inner join mimiciii.patients pat
 on ie.subject_id = pat.subject_id
)
, ca as (
select ch.*
-- callout variables we are interested in
, ca.curr_careunit
, ca.request_tele, ca.request_resp, ca.request_cdiff, ca.request_mrsa, ca.request_vre
, ca.createtime as callouttime
, ca.outcometime
, ca.firstreservationtime
, ca.currentreservationtime
, ca.callout_wardid
, ca.discharge_wardid
, ca.callout_outcome
, ca.callout_status
, coalesce(vent.mechvent,0) as MechVent
, case when ca.subject_id is not null then 1 else 0 end as has_callout_data
-- If a single ICUSTAY_ID has multiple successful callouts, we can consider these as readmissions within 24 hours
-- ICUSTAY_ID will not capture a readmission within 24 hours, so we use the call out discharge time instead
, row_number() over (partition by ch.icustay_id order by ca.createtime) as callout_num
 from ch
left join mimiciii.callout ca
	on ch.subject_id = ca.subject_id and ch.hadm_id = ca.hadm_id
	and ca.createtime between ch.intime and ch.outtime
  and callout_outcome = 'Discharged'
  and callout_status = 'Inactive'
left join
(
  select ie.icustay_id
    , max(case when vent.icustay_id is not null then 1 else 0 end) as MechVent
  from icustays ie
  left join ventdurations vent
    on ie.icustay_id = vent.icustay_id
    and vent.starttime between ie.intime - interval '1' day and ie.intime + interval '1' day
  group by ie.icustay_id
) vent
  on ch.icustay_id = vent.icustay_id
)
-- extract code status at that time
, codestatus1 as
(
  select
    ca.icustay_id
    , case when value = 'Comfort Measures' then 1 else 0 end as CMO
    , case when value = 'Do Not Intubate' then 1 else 0 end as DNI
    , case when substr(value,1,16) = 'CPR Not Indicate' then 1 else 0 end as DNCPR
    , case when substr(value,1,16) = 'Do Not Resuscita' then 1 else 0 end as DNR
    , case when value = 'Full Code' then 1 else 0 end as FULLCODE

    , ROW_NUMBER() OVER (PARTITION BY ca.icustay_id ORDER BY ce.charttime DESC) as rn

  from ca
  left join mimiciii.chartevents ce
    on ca.icustay_id = ce.icustay_id
    and ce.charttime between ca.intime and ca.callouttime
    and ce.itemid in (128,223758)
)
, codestatus2 as
(
  select icustay_id
    , MAX(CMO) as CMO
    , MAX(DNI) as DNI
    , MAX(DNCPR) as DNCPR
    , MAX(DNR) as DNR
    , MAX(FULLCODE) as FULLCODE

    , MAX(case when rn = 1 then CMO else null end) as CMO_LAST
    , MAX(case when rn = 1 then DNI else null end) as DNI_LAST
    , MAX(case when rn = 1 then DNCPR else null end) as DNCPR_LAST
    , MAX(case when rn = 1 then DNR else null end) as DNR_LAST
    , MAX(case when rn = 1 then FULLCODE else null end) as FULLCODE_LAST
  FROM codestatus1
  group by icustay_id
)
select ca.*
-- code status variables
, cs.cmo, cs.cmo_last
, cs.dni, cs.dni_last
, cs.dncpr, cs.dncpr_last
, cs.dnr, cs.dnr_last
, cs.fullcode, cs.fullcode_last

-- actual HHMM of the ICU discharge time
, EXTRACT(HOUR FROM cast(ca.outcometime as timestamp))*100 + EXTRACT(MINUTE FROM cast(ca.outcometime as timestamp)) as HourOfDischarge
, EXTRACT(HOUR FROM cast(ca.callouttime as timestamp))*100 + EXTRACT(MINUTE FROM cast(ca.callouttime as timestamp)) as HourOfCallout
, round(extract(epoch from (ca.outcometime - ca.callouttime))::NUMERIC/60.0/60.0/24.0,4) as DISCHARGEDELAY_DAYS
, round(extract(epoch from (ca.outcometime - ca.intime))::NUMERIC/60.0/60.0/24.0,4) as LOS_ICU_DAYS
, round(extract(epoch from (ca.intime - ca.admittime))::NUMERIC/60.0/60.0/24.0,4) as LOS_PREICU_DAYS
, round(extract(epoch from (ca.dischtime - ca.admittime))::NUMERIC/60.0/60.0/24.0,4) as LOS_HOSPITAL_DAYS
, round(extract(epoch from (ca.dischtime - ca.callouttime))::NUMERIC/60.0/60.0/24.0,4) as LOS_POST_CALLOUT_DAYS
, round(extract(epoch from (ca.dischtime - ca.outcometime))::NUMERIC/60.0/60.0/24.0,4) as LOS_POST_ICU_DAYS
, round(extract(epoch from (ca.callouttime - ca.admittime))::NUMERIC/60.0/60.0/24.0,4) as LOS_PRE_CALLOUT_DAYS
from ca
left join codestatus2 cs
  on ca.icustay_id = cs.icustay_id
where callout_num = 1;



DROP MATERIALIZED VIEW IF EXISTS DD_COHORT CASCADE;
CREATE MATERIALIZED VIEW DD_COHORT AS
select
  dd.*
from dd_cohort_all dd
where
      icustay_num = 1
  and adult = 1
  and has_chartevents_data = 1
  and has_callout_data = 1
  and coalesce(cmo,0) != 1
  and coalesce(cmo_last,0) != 1;

-- print counts
select
  count(*) as num_pat
  , sum(case when adult = 1
             then 1 else 0 end) as adults
  , sum(case when adult = 1
              and has_callout_data = 1
            then 1 else 0 end) as has_callout
  , sum(case when adult = 1
              and has_callout_data = 1
              and icustay_num = 1
            then 1 else 0 end) as first_stay
  , sum(case when adult = 1
              and has_callout_data = 1
              and icustay_num = 1
              and coalesce(cmo,0) != 1 and coalesce(cmo_last,0) != 1
            then 1 else 0 end) as never_cmo
  , sum(case when adult = 1
              and has_callout_data = 1
              and icustay_num = 1
              and coalesce(cmo,0) != 1 and coalesce(cmo_last,0) != 1
              and has_chartevents_data = 1
            then 1 else 0 end) as no_missing_data
from dd_cohort_all dd;
