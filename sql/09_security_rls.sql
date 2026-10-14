-- =========================================================================
-- 09_security_rls.sql
-- Dynamic branch / department row-level security driven by Access_Matrix.
--
-- A user's visibility = UNION of every ACTIVE grant:
--   Enterprise -> everything          Branch -> all departments of that branch
--   Department -> that Branch+Department pair (a user may hold several, across
--                 different branches)
-- Inactive grants (IsActive = 0) never grant access - filtered at the base of
-- the predicate, not left for callers to remember. The matrix is a full
-- snapshot per batch; the latest snapshot wins.
--
-- The grants are EXPANDED once into a small bridge table (user x branch x
-- department) that facts semi-join against, instead of evaluating a
-- correlated predicate per fact row. Same pattern as a Power BI RLS bridge,
-- a SQL Server SECURITY POLICY over an inline TVF, or a Snowflake row access
-- policy backed by a mapping table - only the wrapper syntax changes.
-- =========================================================================
CREATE OR REPLACE VIEW gold.vw_user_entitlement AS
SELECT DISTINCT UserEmail, AccessLevel, Branch, Department
FROM bronze.security_matrix_rls
WHERE IsActive = 1
  AND _batch_id = (SELECT MAX(_batch_id) FROM bronze.security_matrix_rls);

CREATE OR REPLACE TABLE gold.entitlement_expanded AS
WITH bd AS (SELECT DISTINCT Branch, Department, BranchSK, DepartmentSK FROM gold.fact_admission)
SELECT DISTINCT UserEmail, Branch, Department, BranchSK, DepartmentSK FROM (
    SELECT e.UserEmail, bd.* FROM gold.vw_user_entitlement e CROSS JOIN bd WHERE e.AccessLevel = 'Enterprise'
    UNION ALL
    SELECT e.UserEmail, bd.* FROM gold.vw_user_entitlement e JOIN bd ON bd.Branch = e.Branch WHERE e.AccessLevel = 'Branch'
    UNION ALL
    SELECT e.UserEmail, bd.* FROM gold.vw_user_entitlement e
    JOIN bd ON bd.Branch = e.Branch AND bd.Department = e.Department WHERE e.AccessLevel = 'Department'
);

-- Secured access path: what a report/semantic layer runs for the signed-in user.
CREATE OR REPLACE MACRO gold.secured_admission(user_email) AS TABLE
    SELECT f.* FROM gold.fact_admission f
    WHERE EXISTS (SELECT 1 FROM gold.entitlement_expanded e
                  WHERE e.UserEmail = user_email AND e.Branch = f.Branch AND e.Department = f.Department);

-- Example:  SELECT count(*) FROM gold.secured_admission('riyadh.director@mhg.example');
