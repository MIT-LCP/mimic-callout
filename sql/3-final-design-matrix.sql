DROP TABLE IF EXISTS dd_design_matrix;
CREATE TABLE dd_design_matrix as
select
  co.icustay_id

  -- lengths of stay
  , DISCHARGEDELAY_DAYS
  , LOS_HOSPITAL_DAYS -- admittime to dischtime

  , (LOS_HOSPITAL_DAYS - LOS_POST_ICU_DAYS - LOS_ICU_DAYS) as LOS_PREICU_DAYS
  , LOS_ICU_DAYS -- intime to outcometime
  , LOS_POST_ICU_DAYS -- outcometime to dischtime
  , LOS_PRE_CALLOUT_DAYS -- from hospital admit to callout (admittime to callouttime)
  , LOS_POST_CALLOUT_DAYS -- from callout to hospital discharge (callouttime to dischtime)

  -- censor delay
  , round(extract(EPOCH FROM (
      case
        when co.deathtime is null and readm.intime is null then null
        when readm.intime < co.deathtime then readm.intime
        when co.deathtime <= readm.intime then co.deathtime
        else coalesce(co.deathtime, readm.intime)
      end
    - co.callouttime))::NUMERIC /60/60/24, 4) as LOS_POST_CALLOUT_CENSORED_DAYS

  -- things that censor outcome: time of death, time of readmission
  , case when co.deathtime is not null then 1 else 0 end as HospitalDeath
  , case when readm.icustay_id is not null then 1 else 0 end as HospitalReadmission

--  -- censor time
--  , case
--      when co.deathtime is null and readm.intime is null then null
--      when readm.intime < co.deathtime then readm.intime
--      when co.deathtime <= readm.intime then co.deathtime
--      else coalesce(co.deathtime, readm.intime)
--      end as censortime


  -- various factors which could cause call out delays
  , request_tele, request_resp, request_cdiff, request_mrsa, request_vre
  , case when co.gender = 'F' then 1 else 0 end as female
  , oa.oasis
  , elixhauser_hospital

    -- tr.CURR_CAREUNIT is only null for 1 patient
    -- the patient was definitely in the MICU, and this code ensures no missing data !
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'CCU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'CCU' then 1
      else 0 end as CCU
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'CVICU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'CVICU' then 1
      else 0 end as CVICU
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'MICU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'MICU' then 1
      else 0 end as MICU
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'MSICU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'MSICU' then 1
      else 0 end as MSICU
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'SICU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'SICU' then 1
      else 0 end as SICU
  , case
      when tr.CURR_CAREUNIT is not null
        and tr.CURR_CAREUNIT = 'TSICU' then 1
      when tr.CURR_CAREUNIT is null
        and ie.LAST_CAREUNIT = 'TSICU' then 1
      else 0 end as TSICU


  -- if no info given.. assume full code
  , case
      when cmo_last=0
      and dni_last=0
      and dncpr_last=0
      and dnr_last=0
      and fullcode_last=0 then 0
    else fullcode_last
  end as fullcode_last
  , dni_last, dncpr_last, dnr_last

  , HourOfCallout


  -- the rest of the info could be useful so we include it
  , age
  , ethnicity
  , elective
  , case when tr.CURR_CAREUNIT is not null then tr.CURR_CAREUNIT
      when ie.LAST_CAREUNIT is not null then ie.LAST_CAREUNIT
      else null end
      as careunit

  , s.service

  -- if no info given.. assume full code
  , case
      when cmo=0
      and dni=0
      and dncpr=0
      and dnr=0
      and fullcode=0 then 0
    else fullcode end
  as fullcode
  , dni, dncpr, dnr

  , co.callout_wardid -- used for binary flag FirstAvailWard "Yes/No" (callout_wardid=1 is a request for first available ward)
  , co.discharge_wardid
  , to_char(co.callouttime, 'day') as CALLOUT_DAYOFWEEK

from dd_cohort co

-- readmitted before discharged
left join
(
  select co.icustay_id, tr.intime
    , ROW_NUMBER() OVER (partition by tr.hadm_id order by tr.intime) as rn
  from dd_cohort co
  left join mimiciii.transfers tr
  on co.hadm_id = tr.hadm_id
  -- only ICU stays which occurred *after* this ICU's discharge
  and tr.icustay_id is not null and tr.intime > co.outcometime
  where tr.intime is not null -- only readmitted patients
) readm
on co.icustay_id = readm.icustay_id and readm.rn = 1

-- severity of illness
left join dd_oasis oa
  on co.icustay_id = oa.icustay_id

-- care unit
left join mimiciii.transfers tr
  on co.icustay_id = tr.icustay_id and co.callouttime between tr.intime and tr.outtime

-- we join to this table to fix the care unit for literally 1 ICU patient
-- they were called out when the transfer table had them sitting in a ward for an hour, between two stays in MICU
left join mimiciii.icustays ie
  on co.icustay_id = ie.icustay_id

-- service
left join
(
  select co.icustay_id
  , coalesce(s1.only_service, s2.curr_service) as service
  , ROW_NUMBER() over (PARTITION BY CO.ICUSTAY_ID ORDER BY s2.transfertime DESC) as rn

  from dd_cohort co
  left join
  (
    select hadm_id, max(curr_service) as only_service
    from mimiciii.services
    group by hadm_id
    having count(*)=1
  ) s1
  on co.hadm_id = s1.hadm_id
  left join mimiciii.services s2
    on co.hadm_id = s2.hadm_id and s2.transfertime < co.callouttime - interval '4' hour
) s
on co.icustay_id = s.icustay_id and s.rn = 1
left join
(
  select e.*,
    -- in-hospital mortality score
    CONGESTIVE_HEART_FAILURE*(4) + CARDIAC_ARRHYTHMIAS*(4) + VALVULAR_DISEASE*(-3) + PULMONARY_CIRCULATION*(0) +
    PERIPHERAL_VASCULAR     *(0) + HYPERTENSION*(-1) + PARALYSIS*(0) +
    OTHER_NEUROLOGICAL      *(7) + CHRONIC_PULMONARY*(0) +
    DIABETES_UNCOMPLICATED  *(-1) + DIABETES_COMPLICATED*(-4) +
    HYPOTHYROIDISM          *(0) + RENAL_FAILURE*(3) + LIVER_DISEASE*(4) +
    PEPTIC_ULCER            *(-9) + AIDS*(0) + LYMPHOMA*(7) +
    METASTATIC_CANCER       *(9) + SOLID_TUMOR*(0) + RHEUMATOID_ARTHRITIS*(0) +
    COAGULOPATHY*(3) + OBESITY*(-5) + WEIGHT_LOSS*(4)                      + FLUID_ELECTROLYTE*(6) + BLOOD_LOSS_ANEMIA*(0) +
    DEFICIENCY_ANEMIAS      *(-4) + ALCOHOL_ABUSE*(0) + DRUG_ABUSE*(-6) +
    PSYCHOSES               *(-5) + DEPRESSION*(-8)
    AS elixhauser_hospital
  from elixhauser_ahrq e
) eli
  on co.hadm_id = eli.hadm_id

order by icustay_id;
