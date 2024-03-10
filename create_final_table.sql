CREATE OR REPLACE TABLE `data_analysis.cohort.final_table` AS (

WITH

v AS (
    SELECT
        *,
        COUNT(DISTINCT ClientId) OVER(PARTITION BY SalesMonth)TotalSales
    FROM `cohort.sales`
    ORDER BY SalesMonth
),

c AS (
    SELECT
        *
    FROM `cohort.churn`
    ORDER BY ChurnMonth
),

t AS (
    SELECT
        v.SalesMonth,
        c.ChurnMonth,
        v.TotalSales,
        COUNT(DISTINCT c.ClientId)Churn,
        SUM(COUNT(DISTINCT c.ClientId)) OVER(PARTITION BY v.SalesMonth ORDER BY c.ChurnMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)AccumulatedChurn,
        TotalSales-SUM(COUNT(DISTINCT c.ClientId)) OVER(PARTITION BY v.SalesMonth ORDER BY c.ChurnMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)RemainingActives
    FROM v

    LEFT JOIN c
        ON v.ClientId = c.ClientId

    WHERE ChurnMonth IS NOT NULL
    GROUP BY
        v.SalesMonth,
        c.ChurnMonth,
        v.TotalSales
    ORDER BY
        v.SalesMonth,
        c.ChurnMonth
)

SELECT * FROM t
)