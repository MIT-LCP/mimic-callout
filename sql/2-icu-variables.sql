-- Subqueries:
--  1) GCS
--  2) VENT
--  3) Vitals
--  4) Urine output

-- Required tables:
--  dd_cohort

-- Created tables:
--  dd_gcs
--  dd_vitals
--  dd_vent
--  dd_oasis

drop materialized view IF EXISTS dd_gcs;
CREATE MATERIALIZED VIEW dd_gcs as
with base as
(
  SELECT pvt.ICUSTAY_ID, pvt.charttime

  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when GCSTYPE = 'Motor' then pvt.valuenum else null end) as GCSMotor
  , max(case when GCSTYPE = 'Verbal' then pvt.valuenum else null end) as GCSVerbal
  , max(case when GCSTYPE = 'Eyes' then pvt.valuenum else null end) as GCSEyes

  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when GCSTYPE = 'Verbal' then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag

  , ROW_NUMBER ()
          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn

  FROM
  (
    select l.ICUSTAY_ID
    -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
    , case
        when l.ITEMID in (723,223900) then 'Verbal'
        when l.ITEMID in (454,223901) then 'Motor'
        when l.ITEMID in (184,220739) then 'Eyes'
        else null end
      as GCSTYPE

    -- convert the data into a number, reserving a value of 0 for ET/Trach
    , case
        -- endotrach/vent is assigned a value of 0, later parsed specially
        when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
        when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision

        else VALUENUM
        end
      as VALUENUM
    , l.CHARTTIME
    from mimiciii.CHARTEVENTS l

    -- get callout time for charttime subselection
    inner join dd_cohort b
      on l.icustay_id = b.icustay_id

    -- Isolate the desired GCS variables
    where l.ITEMID in
    (
      -- 198 -- GCS
      -- GCS components, CareVue
      184, 454, 723
      -- GCS components, Metavision
      , 223900, 223901, 220739
    )
    -- Only get data for the 24 hours before call out
    and l.charttime between b.callouttime - interval '1' day and b.callouttime
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

  from base b
  -- join to itself within 6 hours to get previous value
  left join base b2
    on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval '6' hour
)
, gcs_final as (
  select gcs.*
  -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
  , ROW_NUMBER ()
          OVER (PARTITION BY gcs.ICUSTAY_ID
                ORDER BY gcs.GCS
               ) as IsMinGCS
  from gcs
)
select ie.ICUSTAY_ID
-- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
, GCS as MinGCS
, coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
, coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
, coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
, EndoTrachFlag as EndoTrachFlag

-- subselect down to the cohort of eligible patients
from dd_cohort ie
left join gcs_final gs
  on ie.ICUSTAY_ID = gs.ICUSTAY_ID
where IsMinGCS = 1;


drop materialized view if exists dd_vitals;
create materialized view dd_vitals as
SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id

-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max

FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose

    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum

  from mimiciii.icustays ie
  left join mimiciii.chartevents ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between ie.intime and ie.intime + interval '1' day
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"

  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"

  -- RESPIRATORY RATE
  618,--	Respiratory Rate
  615,--	Resp Rate (Total)
  220210,--	Respiratory Rate
  224690, --	Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

  -- GLUCOSE, both lab and fingerstick
  807,--	Fingerstick Glucose
  811,--	Glucose (70-105)
  1529,--	Glucose
  3745,--	BloodGlucose
  3744,--	Blood Glucose
  225664,--	Glucose finger stick
  220621,--	Glucose (serum)
  226537,--	Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --	"Temperature F"


  )
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id;

drop materialized view if exists dd_uo;
create materialized view dd_uo AS
select
  -- patient identifiers
  co.subject_id, co.hadm_id, co.icustay_id

  -- volumes associated with urine output ITEMIDs
  , sum(oe.VALUE) as UrineOutput

from dd_cohort co
-- Join to the outputevents table to get urine output
left join mimiciii.outputevents oe
  -- join on all patient identifiers
  on co.subject_id = oe.subject_id and co.hadm_id = oe.hadm_id and co.icustay_id = oe.icustay_id
  -- and ensure the data occurs during the first day
  and oe.charttime between co.callouttime - interval '1' day and co.callouttime -- just before callout
  and itemid in
  (
  -- these are the most frequently occurring urine output observations in CareVue
  40055, -- "Urine Out Foley"
  43175, -- "Urine ."
  40069, -- "Urine Out Void"
  40094, -- "Urine Out Condom Cath"
  40715, -- "Urine Out Suprapubic"
  40473, -- "Urine Out IleoConduit"
  40085, -- "Urine Out Incontinent"
  40057, -- "Urine Out Rt Nephrostomy"
  40056, -- "Urine Out Lt Nephrostomy"
  40405, -- "Urine Out Other"
  40428, -- "Urine Out Straight Cath"
  40086,--	Urine Out Incontinent
  40096, -- "Urine Out Ureteral Stent #1"
  40651, -- "Urine Out Ureteral Stent #2"

  -- these are the most frequently occurring urine output observations in CareVue
  226559, -- "Foley"
  226560, -- "Void"
  227510, -- "TF Residual"
  226561, -- "Condom Cath"
  226584, -- "Ileoconduit"
  226563, -- "Suprapubic"
  226564, -- "R Nephrostomy"
  226565, -- "L Nephrostomy"
  226567, --	Straight Cath
  226557, -- "R Ureteral Stent"
  226558  -- "L Ureteral Stent"
  )
group by co.subject_id, co.hadm_id, co.icustay_id, co.callouttime
order by co.icustay_id;


drop materialized view if exists dd_vent;
create materialized view dd_vent as

select
  co.subject_id, co.hadm_id, co.icustay_id
-- use ventilator settings to determine existence of mechanical ventilation
-- case statement determining whether it is an instance of mech vent
, max(
  case
    when itemid is null or value is null then 0 -- can't have null values
    when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
    when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
    when itemid = 648 and value = 'Intubated/trach' THEN 1 -- Speech = intubated
    when itemid in
      (
      445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
      , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
      , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
      , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
      , 543 -- PlateauPressure
      , 5865,5866,224707,224709,224705,224706 -- APRV pressure
      , 60,437,505,506,686,220339,224700 -- PEEP
      , 3459 -- high pressure relief
      , 501,502,503,224702 -- PCV
      , 223,667,668,669,670,671,672 -- TCPCV
      , 157,158,1852,3398,3399,3400,3401,3402,3403,3404,8382,227809,227810 -- ETT
      , 224701 -- PSVlevel
      )
      THEN 1
    else 0
  end
  ) as MechVent
from dd_cohort co
left join mimiciii.chartevents tt
  on co.subject_id = tt.subject_id and co.hadm_id = tt.hadm_id and co.icustay_id = tt.icustay_id
  and tt.charttime between co.callouttime - interval '1' day and co.callouttime
group by co.subject_id, co.hadm_id, co.icustay_id
order by co.icustay_id;




-- now, using the above data, extract a severity score for the 24 hours before discharge
DROP MATERIALIZED VIEW IF EXISTS DD_OASIS;
CREATE MATERIALIZED VIEW DD_OASIS as

with surgflag as
(
  select ie.icustay_id
    , max(case
        when lower(curr_service) like '%surg%' then 1
        when curr_service = 'ORTHO' then 1
    else 0 end) as surgical
  from mimiciii.icustays ie
  left join mimiciii.services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
  group by ie.icustay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime
      , adm.deathtime
      , cast(ie.intime as timestamp) - cast(adm.admittime as timestamp) as PreICULOS
      , floor( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) as age
      , gcs.mingcs
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.meanbp_max
      , vital.meanbp_min
      , vital.resprate_max
      , vital.resprate_min
      , vital.tempc_max
      , vital.tempc_min
      , vent.mechvent
      , uo.urineoutput

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 1
          when adm.ADMISSION_TYPE is null or sf.surgical is null
            then null
          else 0
        end as ElectiveSurgery

      -- age group
      , case
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= (60*60*24*12) then 'neonate'
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= (60*60*24*12*15) then 'middle'
        else 'adult' end as ICUSTAY_AGE_GROUP

      -- mortality flags
      , case
          when adm.deathtime between ie.intime and ie.outtime
            then 1
          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
            then 1
          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
            then 1
          else 0 end
        as ICUSTAY_EXPIRE_FLAG
      , adm.hospital_expire_flag
from mimiciii.icustays ie
inner join mimiciii.admissions adm
  on ie.hadm_id = adm.hadm_id
inner join mimiciii.patients pat
  on ie.subject_id = pat.subject_id
left join surgflag sf
  on ie.icustay_id = sf.icustay_id
-- join to custom tables to get more data....
left join dd_gcs gcs
  on ie.icustay_id = gcs.icustay_id
left join dd_vitals vital
  on ie.icustay_id = vital.icustay_id
left join dd_uo uo
  on ie.icustay_id = uo.icustay_id
left join dd_vent vent
  on ie.icustay_id = vent.icustay_id
)
, scorecomp as
(
select co.subject_id, co.hadm_id, co.icustay_id
, co.ICUSTAY_AGE_GROUP
, co.icustay_expire_flag
, co.hospital_expire_flag

-- Below code calculates the component scores needed for OASIS
, case when preiculos is null then null
     when preiculos < '0 0:10:12' then 5
     when preiculos < '0 4:57:00' then 3
     when preiculos < '1 0:00:00' then 0
     when preiculos < '12 23:48:00' then 1
     else 2 end as preiculos_score
,  case when age is null then null
      when age < 24 then 0
      when age <= 53 then 3
      when age <= 77 then 6
      when age <= 89 then 9
      when age >= 90 then 7
      else 0 end as age_score
,  case when mingcs is null then null
      when mingcs <= 7 then 10
      when mingcs < 14 then 4
      when mingcs = 14 then 3
      else 0 end as gcs_score
,  case when heartrate_max is null then null
      when heartrate_max > 125 then 6
      when heartrate_min < 33 then 4
      when heartrate_max >= 107 and heartrate_max <= 125 then 3
      when heartrate_max >= 89 and heartrate_max <= 106 then 1
      else 0 end as heartrate_score
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then 4
      when meanbp_min < 51 then 3
      when meanbp_max > 143.44 then 3
      when meanbp_min >= 51 and meanbp_min < 61.33 then 2
      else 0 end as meanbp_score
,  case when resprate_min is null then null
      when resprate_min <   6 then 10
      when resprate_max >  44 then  9
      when resprate_max >  30 then  6
      when resprate_max >  22 then  1
      when resprate_min <  13 then 1 else 0
      end as resprate_score
,  case when tempc_max is null then null
      when tempc_max > 39.88 then 6
      when tempc_min >= 33.22 and tempc_min <= 35.93 then 4
      when tempc_max >= 33.22 and tempc_max <= 35.93 then 4
      when tempc_min < 33.22 then 3
      when tempc_min > 35.93 and tempc_min <= 36.39 then 2
      when tempc_max >= 36.89 and tempc_max <= 39.88 then 2
      else 0 end as temp_score
,  case when UrineOutput is null then null
      when UrineOutput < 671.09 then 10
      when UrineOutput > 6896.80 then 8
      when UrineOutput >= 671.09
       and UrineOutput <= 1426.99 then 5
      when UrineOutput >= 1427.00
       and UrineOutput <= 2544.14 then 1
      else 0 end as UrineOutput_score
,  case when mechvent is null then null
      when mechvent = 1 then 9
      else 0 end as mechvent_score
,  case when ElectiveSurgery is null then null
      when ElectiveSurgery = 1 then 0
      else 6 end as electivesurgery_score

-- The below code gives the component associated with each score
-- This is not needed to calculate OASIS, but provided for user convenience.
-- If both the min/max are in the normal range (score of 0), then the average value is stored.
, preiculos
, age
, mingcs as gcs
,  case when heartrate_max is null then null
      when heartrate_max > 125 then heartrate_max
      when heartrate_min < 33 then heartrate_min
      when heartrate_max >= 107 and heartrate_max <= 125 then heartrate_max
      when heartrate_max >= 89 and heartrate_max <= 106 then heartrate_max
      else (heartrate_min+heartrate_max)/2 end as heartrate
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then meanbp_min
      when meanbp_min < 51 then meanbp_min
      when meanbp_max > 143.44 then meanbp_max
      when meanbp_min >= 51 and meanbp_min < 61.33 then meanbp_min
      else (meanbp_min+meanbp_max)/2 end as meanbp
,  case when resprate_min is null then null
      when resprate_min <   6 then resprate_min
      when resprate_max >  44 then resprate_max
      when resprate_max >  30 then resprate_max
      when resprate_max >  22 then resprate_max
      when resprate_min <  13 then resprate_min
      else (resprate_min+resprate_max)/2 end as resprate
,  case when tempc_max is null then null
      when tempc_max > 39.88 then tempc_max
      when tempc_min >= 33.22 and tempc_min <= 35.93 then tempc_min
      when tempc_max >= 33.22 and tempc_max <= 35.93 then tempc_max
      when tempc_min < 33.22 then tempc_min
      when tempc_min > 35.93 and tempc_min <= 36.39 then tempc_min
      when tempc_max >= 36.89 and tempc_max <= 39.88 then tempc_max
      else (tempc_min+tempc_max)/2 end as temp
,  UrineOutput
,  mechvent
,  ElectiveSurgery
from cohort co
)
, score as
(
select s.*
    , coalesce(age_score,0)
    + coalesce(preiculos_score,0)
    + coalesce(gcs_score,0)
    + coalesce(heartrate_score,0)
    + coalesce(meanbp_score,0)
    + coalesce(resprate_score,0)
    + coalesce(temp_score,0)
    + coalesce(urineoutput_score,0)
    + coalesce(mechvent_score,0)
    + coalesce(electivesurgery_score,0)
    as OASIS
from scorecomp s
)
select
  subject_id, hadm_id, icustay_id
  -- , ICUSTAY_AGE_GROUP
  -- , hospital_expire_flag
  -- , icustay_expire_flag
  , OASIS
  -- Calculate the probability of in-hospital mortality
  , 1 / (1 + exp(- (-6.1746 + 0.1275*(OASIS) ))) as OASIS_PROB
  -- , age, age_score
  -- , preiculos, preiculos_score
  -- , gcs, gcs_score
  -- , heartrate, heartrate_score
  -- , meanbp, meanbp_score
  -- , resprate, resprate_score
  -- , temp, temp_score
  -- , urineoutput, UrineOutput_score
  -- , mechvent, mechvent_score
  -- , electivesurgery, electivesurgery_score
from score
order by icustay_id;
