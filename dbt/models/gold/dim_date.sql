WITH bounds AS (
    SELECT date_trunc('year', LEAST((SELECT MIN(CAST(AdmissionDateTime AS DATE)) FROM {{ ref('admission_trusted') }}),
                                    (SELECT MIN(ServiceDate) FROM {{ source('bronze', 'billing_transactions') }}))) AS d0,
           date_trunc('year', (SELECT MAX(CAST(AdmissionDateTime AS DATE)) FROM {{ ref('admission_trusted') }}))
               + INTERVAL 2 YEAR - INTERVAL 1 DAY AS d1
)
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS DateSK, CAST(d AS DATE) AS CalendarDate, year(d) AS Year, quarter(d) AS Quarter,
       month(d) AS Month, strftime(d, '%b') AS MonthName, CAST(date_trunc('month', d) AS DATE) AS MonthStart,
       dayofweek(d) AS DayOfWeek, CASE WHEN dayofweek(d) IN (5, 6) THEN 1 ELSE 0 END AS IsWeekend
FROM bounds, generate_series(d0, d1, INTERVAL 1 DAY) AS t(d)
