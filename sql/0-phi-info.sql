-- Information that contains PHI (i.e. dates, census info)
-- These queries must be run in Oracle connected to the PHI database

-- step 1) define dd_cohort exactly as it is done in 1-define-cohort.sql
with ch as (
select ie.subject_id, ie.hadm_id, ie.icustay_id
-- ICU admission/discharge times
, ie.intime, ie.outtime

-- exclusions
, case -- not a child
    when round( (cast(ie.intime as date) - cast(pat.dob as date)) /365.242,4) > 15
      then 1
    else 0
  end as adult
, adm.has_chartevents_data -- hospital admission with data

-- Age at ICU admission
, case when round( (cast(ie.intime as date) - cast(pat.dob as date))/365.242,4 ) > 200 then 91.4
else round( (cast(ie.intime as date) - cast(pat.dob as date))/365.242,4 ) end as age

-- An integer which is 1 for the first ICU stay of the hospitalization
-- Increases by 1 for each subsequent ICU admission
, row_number() over (partition by ie.subject_id, ie.hadm_id order by ie.intime) as icustay_num

from mimiciii_phi.icustays ie
inner join mimiciii_phi.admissions adm
 on ie.hadm_id = adm.hadm_id
inner join mimiciii_phi.patients pat
 on ie.subject_id = pat.subject_id
)
, ca as (
select ch.subject_id, ch.hadm_id, ch.icustay_id
, ch.intime, ch.outtime
, ch.age

-- callout variables we are interested in
, ca.createtime as callouttime
, ca.outcometime

, ca.callout_wardid
, ca.discharge_wardid

-- exclusions
, ch.adult
, ch.has_chartevents_data
, ch.icustay_num
, case when ca.subject_id is not null then 1 else 0 end as has_callout_data

-- If a single ICUSTAY_ID has multiple successful callouts, we can consider these as readmissions within 24 hours
-- ICUSTAY_ID will not capture a readmission within 24 hours, so we use the call out discharge time instead
, row_number() over (partition by ch.icustay_id order by ca.createtime) as callout_num
from ch
left join mimiciii_phi.callout ca
	on ch.subject_id = ca.subject_id and ch.hadm_id = ca.hadm_id
	and ca.createtime between ch.intime and ch.outtime
  and callout_outcome = 'Discharged'
  and callout_status = 'Inactive'
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
  left join mimiciii_phi.chartevents ce
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
-- the below should be identical to the cohort defined in 1-define-cohort.sql
-- .. except, of course, it has fewer columns
, dd as
(
select ca.*
from ca
left join codestatus2 cs
  on ca.icustay_id = cs.icustay_id
where icustay_num = 1
and adult = 1
and has_chartevents_data = 1
and has_callout_data = 1
and coalesce(cmo,0) != 1
and coalesce(cmo_last,0) != 1
and callout_num = 1
)
------------------------
-- ADD IN CENSUS DATA --
------------------------
, t1 as
(
select
  census_date
  , sum(case when cost_center_desc in ('CC6A - Med/Surg/Trauma', 'Acute Surgical/Trauma') then CENSUS else null end) as MedSurgTrauma_CENSUS
  , sum(case when cost_center_desc in ('CC6A - Med/Surg/Trauma', 'Acute Surgical/Trauma') then BEDS else null end) as MedSurgTrauma_BEDS
  , sum(case when cost_center_desc in ('RS12 - Med/Surg/GYN ', 'Medical/ Surgical (Gynecology) Inpatient Unit') then CENSUS else null end) as MedSurgGynecology_CENSUS
  , sum(case when cost_center_desc in ('RS12 - Med/Surg/GYN ', 'Medical/ Surgical (Gynecology) Inpatient Unit') then BEDS else null end) as MedSurgGynecology_BEDS
  , sum(case when cost_center_desc in ('RS11 - Hem/Onc', 'FD7 - Hem/Onc', 'Hematology /Oncology Inpatient Unit' ) then CENSUS else null end) as HemOnc_CENSUS
  , sum(case when cost_center_desc in ('RS11 - Hem/Onc', 'FD7 - Hem/Onc', 'Hematology /Oncology Inpatient Unit' ) then BEDS else null end) as HemOnc_BEDS
  , sum(case when cost_center_desc in ('FA10 - Transplant', 'Transplant Unit') then CENSUS else null end) as TransplantUnit_CENSUS
  , sum(case when cost_center_desc in ('FA10 - Transplant', 'Transplant Unit') then BEDS else null end) as TransplantUnit_BEDS
  , sum(case when cost_center_desc in ('FA11 - Neuro', 'Neurology/ Neurosurgery') then CENSUS else null end) as Neuro_CENSUS
  , sum(case when cost_center_desc in ('FA11 - Neuro', 'Neurology/ Neurosurgery') then BEDS else null end) as Neuro_BEDS
  , sum(case when cost_center_desc in ('FA3 - Cardiology/Medicine', 'Cardiology/ Medical Inpatient Unit') then CENSUS else null end) as CardiologyMed_CENSUS
  , sum(case when cost_center_desc in ('FA3 - Cardiology/Medicine', 'Cardiology/ Medical Inpatient Unit') then BEDS else null end) as CardiologyMed_BEDS
  , sum(case when cost_center_desc in ('FA5 - Vascular', 'Vascular') then CENSUS else null end) as Vascular_CENSUS
  , sum(case when cost_center_desc in ('FA5 - Vascular', 'Vascular') then BEDS else null end) as Vascular_BEDS
  , sum(case when cost_center_desc in ('FA6A - Cardiac Surgery ', 'Cardiac Surgery') then CENSUS else null end) as CardiacSurgery_CENSUS
  , sum(case when cost_center_desc in ('FA6A - Cardiac Surgery ', 'Cardiac Surgery') then BEDS else null end) as CardiacSurgery_BEDS
  , sum(case when cost_center_desc in ('CC7A - Medicine ', 'FA2 - Medicine', 'Medicine Inpatient Unit', 'Medicine' , 'ST5 - Medicine ') then CENSUS else null end) as Medicine_CENSUS
  , sum(case when cost_center_desc in ('CC7A - Medicine ', 'FA2 - Medicine', 'Medicine Inpatient Unit', 'Medicine' , 'ST5 - Medicine ') then BEDS else null end) as Medicine_BEDS
  , sum(case when cost_center_desc in ('FA7 - Med/Surg', 'ST7 - Med/Surg ', 'Med/ Surg') then CENSUS else null end) as MedSurg_CENSUS
  , sum(case when cost_center_desc in ('FA7 - Med/Surg', 'ST7 - Med/Surg ', 'Med/ Surg') then BEDS else null end) as MedSurg_BEDS
  , sum(case when cost_center_desc in ('Surgery/ PancreaticBiliary/Bariatric', 'FA9 - Med/Sug') then CENSUS else null end) as MedSurgPancrBiliary_CENSUS
  , sum(case when cost_center_desc in ('Surgery/ PancreaticBiliary/Bariatric', 'FA9 - Med/Sug') then BEDS else null end) as MedSurgPancrBiliary_BEDS

  , sum(case when cost_center_desc = 'Emergency Department' then CENSUS else null end) as EmergencyDepartment_CENSUS
  , sum(case when cost_center_desc = 'Emergency Department' then BEDS else null end) as EmergencyDepartment_BEDS
  , sum(case when cost_center_desc = 'Labor & Delivery' then CENSUS else null end) as LaborDelivery_CENSUS
  , sum(case when cost_center_desc = 'Labor & Delivery' then BEDS else null end) as LaborDelivery_BEDS
  , sum(case when cost_center_desc = 'Medical/ Cardiology' then CENSUS else null end) as MedicalCardiology_CENSUS
  , sum(case when cost_center_desc = 'Medical/ Cardiology' then BEDS else null end) as MedicalCardiology_BEDS
  , sum(case when cost_center_desc = 'Obstetrics (Postpartum & Antepartum) Inpatient Unit' then CENSUS else null end) as Obstetrics_CENSUS
  , sum(case when cost_center_desc = 'Obstetrics (Postpartum & Antepartum) Inpatient Unit' then BEDS else null end) as Obstetrics_BEDS
  , sum(case when cost_center_desc = 'PACU - West' then CENSUS else null end) as PACUWest_CENSUS
  , sum(case when cost_center_desc = 'PACU - West' then BEDS else null end) as PACUWest_BEDS
  , sum(case when cost_center_desc = 'Thoracic Surgery' then CENSUS else null end) as ThoracicSurgery_CENSUS
  , sum(case when cost_center_desc = 'Thoracic Surgery' then BEDS else null end) as ThoracicSurgery_BEDS
  , sum(case when cost_center_desc = 'Vascular stepdown' then CENSUS else null end) as VascularStepdown_CENSUS
  , sum(case when cost_center_desc = 'Vascular stepdown' then BEDS else null end) as VascularStepdown_BEDS

from
(
select census_date, floor_name
  , census, beds, other
  , cc.floor, cc.type
  , cc.cost_center_desc
from mimiciii_phi.a_census_unpivot cen
left join MIMICIII_PHI.A_COST_CENTER cc
  on cen.census_date = cc.activity_dt
  and upper(trim(cen.floor_name)) = upper(trim(cc.floor))
) cen
group by census_date
)
-- create table 2, which defines whether a ward is a member of a particular cost center on a given day
, t2 as
(
select
    activity_dt
  , upper(trim(floor)) as FLOOR
  , type
  , case
    -- below cost centers are merged together as the name changes are mostly cosmetic
    -- i.e., the ward name was added to the cost center, etc
     when cost_center_desc in ('CC6A - Med/Surg/Trauma', 'Acute Surgical/Trauma') then 'MedSurgTrauma'
     when cost_center_desc in ('RS12 - Med/Surg/GYN ', 'Medical/ Surgical (Gynecology) Inpatient Unit') then 'MedSurgGynecology'
     when cost_center_desc in ('RS11 - Hem/Onc', 'FD7 - Hem/Onc', 'Hematology /Oncology Inpatient Unit' ) then 'HemOnc'
     when cost_center_desc in ('FA10 - Transplant', 'Transplant Unit') then 'TransplantUnit'
     when cost_center_desc in ('FA11 - Neuro', 'Neurology/ Neurosurgery') then 'Neuro'
     when cost_center_desc in ('FA3 - Cardiology/Medicine', 'Cardiology/ Medical Inpatient Unit') then 'CardiologyMed'
     when cost_center_desc in ('FA5 - Vascular', 'Vascular') then 'Vascular'
     when cost_center_desc in ('FA6A - Cardiac Surgery ', 'Cardiac Surgery') then 'CardiacSurgery'
     when cost_center_desc in ('CC7A - Medicine ', 'FA2 - Medicine', 'Medicine Inpatient Unit', 'Medicine' , 'ST5 - Medicine ') then 'Medicine'
     when cost_center_desc in ('FA7 - Med/Surg', 'ST7 - Med/Surg ', 'Med/ Surg') then 'MedSurg'
     when cost_center_desc in ('Surgery/ PancreaticBiliary/Bariatric', 'FA9 - Med/Sug') then 'MedSurgPancrBiliary'

     -- below cost centers aren't merged, but their names are changed to match the above column names
    when cost_center_desc = 'Emergency Department' then 'EmergencyDepartment'
    when cost_center_desc = 'Labor & Delivery' then 'LaborDelivery'
    when cost_center_desc = 'Medical/ Cardiology' then 'MedicalCardiology'
    when cost_center_desc = 'Obstetrics (Postpartum & Antepartum) Inpatient Unit' then 'Obstetrics'
    when cost_center_desc = 'PACU - West' then 'PACUWest'
    when cost_center_desc = 'Thoracic Surgery' then 'ThoracicSurgery'
    when cost_center_desc = 'Vascular stepdown' then 'VascularStepdown'

    when cost_center_desc is null then 'MISSING_COST_CENTER'
  else
    cost_center_desc
  end as cost_center
from MIMICIII_PHI.A_COST_CENTER
)


-- final table - join callout data to above two tables, adding:
--    1) the census of the hospital (table 1)
--    2) the cost center type of the callout ward (table 2)
select
  -- variables from dd_design_matrix
    dd.icustay_id
  , dd.age
  , to_char( dd.callouttime, 'mm') as CALLOUT_MONTH
  , to_char( dd.callouttime, 'yyyy') as CALLOUT_YEAR

  -- cost center info for the callout ward
  , case when t2.activity_dt is not null then 1 else 0 end as HAS_WARD_INFO
  , t2.cost_center
  , t2.type as ward_type

  -- census variables
  , case when t1.CENSUS_DATE is not null then 1 else 0 end as HAS_CENSUS
  , t1.MedSurgTrauma_CENSUS
  , t1.MedSurgTrauma_BEDS
  , t1.MedSurgGynecology_CENSUS
  , t1.MedSurgGynecology_BEDS
  , t1.HemOnc_CENSUS
  , t1.HemOnc_BEDS
  , t1.TransplantUnit_CENSUS
  , t1.TransplantUnit_BEDS
  , t1.Neuro_CENSUS
  , t1.Neuro_BEDS
  , t1.CardiologyMed_CENSUS
  , t1.CardiologyMed_BEDS
  , t1.Vascular_CENSUS
  , t1.Vascular_BEDS
  , t1.CardiacSurgery_CENSUS
  , t1.CardiacSurgery_BEDS
  , t1.Medicine_CENSUS
  , t1.Medicine_BEDS
  , t1.MedSurg_CENSUS
  , t1.MedSurg_BEDS
  , t1.MedSurgPancrBiliary_CENSUS
  , t1.MedSurgPancrBiliary_BEDS

  , t1.EmergencyDepartment_CENSUS
  , t1.EmergencyDepartment_BEDS
  , t1.LaborDelivery_CENSUS
  , t1.LaborDelivery_BEDS
  , t1.MedicalCardiology_CENSUS
  , t1.MedicalCardiology_BEDS
  , t1.Obstetrics_CENSUS
  , t1.Obstetrics_BEDS
  , t1.PACUWest_CENSUS
  , t1.PACUWest_BEDS
  , t1.ThoracicSurgery_CENSUS
  , t1.ThoracicSurgery_BEDS
  , t1.VascularStepdown_CENSUS
  , t1.VascularStepdown_BEDS

from dd
inner join mimiciii_phi.D_WARD dw
  on dd.callout_wardid = dw.wardid
-- join to get the census for the day
left join t1
  on trunc(dd.callouttime,'dd') = t1.census_date
left join t2
  on trunc(dd.callouttime,'dd') = t2.activity_dt
  and upper(trim(dw.ward)) = t2.floor
order by dd.icustay_id;
