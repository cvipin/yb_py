-- 1. History table (Normalized: ID1 < ID2)
CREATE TABLE PME_MTCH_HIST (
    ID1          NUMBER NOT NULL,
    ID2          NUMBER NOT NULL,
    CREATED_DT   DATE DEFAULT SYSDATE,
    CONSTRAINT PK_PME_MTCH_HIST PRIMARY KEY (ID1, ID2)
);

-- 2. Final Cluster Output table (for auditing / downstream visibility)
CREATE TABLE PME_CLUSTER_RESULT (
    CLUSTER_ROOT NUMBER NOT NULL,
    NODE_ID      NUMBER NOT NULL,
    UPDATED_DT   DATE DEFAULT SYSDATE,
    CONSTRAINT PK_PME_CLUSTER_RESULT PRIMARY KEY (NODE_ID)
);
CREATE INDEX IDX_PME_CLUST_ROOT ON PME_CLUSTER_RESULT(CLUSTER_ROOT);

-- 3. GTT for Active Undirected Edges in the current run
CREATE GLOBAL TEMPORARY TABLE GTT_ACTIVE_EDGES (
    U_NODE NUMBER NOT NULL,
    V_NODE NUMBER NOT NULL
) ON COMMIT PRESERVE ROWS;
CREATE INDEX IDX_GTT_EDGES_U ON GTT_ACTIVE_EDGES(U_NODE, V_NODE);

-- 4. GTT for Iterative Cluster Convergence
CREATE GLOBAL TEMPORARY TABLE GTT_NODE_CLUSTER (
    NODE_ID      NUMBER PRIMARY KEY,
    CLUSTER_ROOT NUMBER NOT NULL
) ON COMMIT PRESERVE ROWS;

-- 5. GTT for Day 2+ Impacted Nodes & Dependencies
CREATE GLOBAL TEMPORARY TABLE GTT_IMPACTED_NODES (
    NODE_ID NUMBER PRIMARY KEY
) ON COMMIT PRESERVE ROWS;

-- GTT to store active unmerge constraints (ANCHOR_NODE < TARGET_NODE)
CREATE GLOBAL TEMPORARY TABLE GTT_UNMERGE_RULES (
    ANCHOR_NODE NUMBER NOT NULL,
    TARGET_NODE NUMBER NOT NULL,
    CONSTRAINT PK_GTT_UNMERGE_RULES PRIMARY KEY (ANCHOR_NODE, TARGET_NODE)
) ON COMMIT PRESERVE ROWS;

-- Optional: Audit table to log transitive edges broken by an unmerge rule
CREATE TABLE PME_TRANSITIVE_CUT_LOG (
    UNMERGE_ANCHOR NUMBER NOT NULL,
    UNMERGE_TARGET NUMBER NOT NULL,
    CUT_NODE_1     NUMBER NOT NULL,
    CUT_NODE_2     NUMBER NOT NULL,
    LOGGED_DT      DATE DEFAULT SYSDATE
);


CREATE OR REPLACE PROCEDURE PRC_RUN_MDM_MERGE (
    p_run_mode IN VARCHAR2 DEFAULT 'DELTA' -- Pass 'DAY1' or 'DELTA'
) AS

    -- Internal helper to run Min-Label Propagation + Pointer Jumping
    -- When p_exclude_targets = TRUE, edges touching unmerge TARGET_NODEs are withheld
    PROCEDURE run_label_propagation (p_exclude_targets IN BOOLEAN) IS
        v_rows_updated NUMBER := 1;
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE GTT_NODE_CLUSTER';

        INSERT /*+ APPEND */ INTO GTT_NODE_CLUSTER (NODE_ID, CLUSTER_ROOT)
        SELECT NODE_ID, NODE_ID
          FROM GTT_IMPACTED_NODES;
        COMMIT;

        WHILE v_rows_updated > 0 LOOP
            -- Neighbour Min-Label Propagation
            IF p_exclude_targets THEN
                MERGE INTO GTT_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM GTT_ACTIVE_EDGES e
                      JOIN GTT_NODE_CLUSTER nc ON e.V_NODE = nc.NODE_ID
                     WHERE NOT EXISTS (
                           SELECT 1 FROM GTT_UNMERGE_RULES r
                            WHERE r.TARGET_NODE = e.U_NODE
                               OR r.TARGET_NODE = e.V_NODE
                     )
                     GROUP BY e.U_NODE
                ) src
                ON (c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            ELSE
                MERGE INTO GTT_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM GTT_ACTIVE_EDGES e
                      JOIN GTT_NODE_CLUSTER nc ON e.V_NODE = nc.NODE_ID
                     GROUP BY e.U_NODE
                ) src
                ON (c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            END IF;

            v_rows_updated := SQL%ROWCOUNT;
            COMMIT;

            -- Pointer Jumping (Root Compression)
            MERGE INTO GTT_NODE_CLUSTER c
            USING GTT_NODE_CLUSTER p
            ON (c.CLUSTER_ROOT = p.NODE_ID)
            WHEN MATCHED THEN
                UPDATE SET c.CLUSTER_ROOT = p.CLUSTER_ROOT
                 WHERE p.CLUSTER_ROOT < c.CLUSTER_ROOT;

            v_rows_updated := v_rows_updated + SQL%ROWCOUNT;
            COMMIT;
        END LOOP;
    END run_label_propagation;

BEGIN
    -- Clear session GTTs
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTT_ACTIVE_EDGES';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTT_NODE_CLUSTER';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTT_IMPACTED_NODES';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTT_UNMERGE_RULES';

    ---------------------------------------------------------------------------
    -- STEP 1: SEED & ACCUMULATE PME_MTCH_HIST
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        MERGE INTO PME_MTCH_HIST h
        USING (
            SELECT DISTINCT
                   LEAST(ROWID_OBJECT, ORIG_ROWID_OBJECT)    AS ID1,
                   GREATEST(ROWID_OBJECT, ORIG_ROWID_OBJECT) AS ID2
              FROM PARTY_XREF
             WHERE ROWID_OBJECT <> ORIG_ROWID_OBJECT
        ) x
        ON (h.ID1 = x.ID1 AND h.ID2 = x.ID2)
        WHEN NOT MATCHED THEN
            INSERT (ID1, ID2, CREATED_DT) VALUES (x.ID1, x.ID2, SYSDATE);
    END IF;

    MERGE INTO PME_MTCH_HIST h
    USING (
        SELECT DISTINCT
               LEAST(ID1, ID2)    AS ID1,
               GREATEST(ID1, ID2) AS ID2
          FROM PME_MTCH
         WHERE ID1 <> ID2
    ) m
    ON (h.ID1 = m.ID1 AND h.ID2 = m.ID2)
    WHEN NOT MATCHED THEN
        INSERT (ID1, ID2, CREATED_DT) VALUES (m.ID1, m.ID2, SYSDATE);

    COMMIT;

    ---------------------------------------------------------------------------
    -- STEP 2: BUILD SUBGRAPH & LOAD UNMERGE RULES
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        -- Load all active unmerge rules
        INSERT INTO GTT_UNMERGE_RULES (ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT LEAST(ID1, ID2), GREATEST(ID1, ID2)
          FROM PME_MANUAL_OVERRIDES
         WHERE ISUNMERGE = 1 AND ID1 <> ID2;

        -- Load candidate edges (excluding direct unmerges)
        INSERT /*+ APPEND */ INTO GTT_ACTIVE_EDGES (U_NODE, V_NODE)
        WITH EFFECTIVE_PAIRS AS (
            SELECT ID1, ID2 FROM PME_MTCH_HIST
            UNION
            SELECT LEAST(ID1, ID2), GREATEST(ID1, ID2)
              FROM PME_MANUAL_OVERRIDES
             WHERE ISMERGE = 1 AND ID1 <> ID2
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM GTT_UNMERGE_RULES
        )
        SELECT ID1, ID2 FROM EFFECTIVE_PAIRS
        UNION ALL
        SELECT ID2, ID1 FROM EFFECTIVE_PAIRS;

        COMMIT;

        INSERT /*+ APPEND */ INTO GTT_IMPACTED_NODES (NODE_ID)
        SELECT U_NODE FROM GTT_ACTIVE_EDGES
        UNION
        SELECT ANCHOR_NODE FROM GTT_UNMERGE_RULES
        UNION
        SELECT TARGET_NODE FROM GTT_UNMERGE_RULES
        UNION
        SELECT ORIG_ROWID_OBJECT FROM PARTY_XREF WHERE ROWID_OBJECT <> ORIG_ROWID_OBJECT
        UNION
        SELECT ROWID_OBJECT FROM PARTY_XREF WHERE ROWID_OBJECT <> ORIG_ROWID_OBJECT;

        COMMIT;

    ELSE
        -- DAY 2+ (DELTA): Seed impacted nodes from PME_MTCH and PME_MANUAL_OVERRIDES
        INSERT INTO GTT_IMPACTED_NODES (NODE_ID)
        SELECT ID1 FROM PME_MTCH WHERE ID1 <> ID2
        UNION
        SELECT ID2 FROM PME_MTCH WHERE ID1 <> ID2
        UNION
        SELECT ID1 FROM PME_MANUAL_OVERRIDES WHERE (ISMERGE = 1 OR ISUNMERGE = 1)
        UNION
        SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE (ISMERGE = 1 OR ISUNMERGE = 1);

        -- Expand cluster and match history dependencies iteratively
        LOOP
            INSERT INTO GTT_IMPACTED_NODES (NODE_ID)
            WITH NEW_DEPS AS (
                SELECT x.ORIG_ROWID_OBJECT AS DEP_NODE
                  FROM PARTY_XREF x
                 WHERE x.ROWID_OBJECT IN (
                       SELECT p.ROWID_OBJECT
                         FROM PARTY_XREF p
                         JOIN GTT_IMPACTED_NODES i ON p.ORIG_ROWID_OBJECT = i.NODE_ID
                       UNION
                       SELECT NODE_ID FROM GTT_IMPACTED_NODES
                 )
                UNION
                SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END AS DEP_NODE
                  FROM PME_MTCH_HIST h
                  JOIN GTT_IMPACTED_NODES i
                    ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID)
            )
            SELECT DISTINCT d.DEP_NODE
              FROM NEW_DEPS d
             WHERE NOT EXISTS (
                   SELECT 1 FROM GTT_IMPACTED_NODES e WHERE e.NODE_ID = d.DEP_NODE
             );

            EXIT WHEN SQL%ROWCOUNT = 0;
        END LOOP;

        -- Load unmerge rules relevant to the impacted subgraph
        INSERT INTO GTT_UNMERGE_RULES (ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT LEAST(o.ID1, o.ID2), GREATEST(o.ID1, o.ID2)
          FROM PME_MANUAL_OVERRIDES o
         WHERE o.ISUNMERGE = 1
           AND o.ID1 <> o.ID2
           AND (EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = o.ID1)
             OR EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = o.ID2));

        -- Build candidate edges for the impacted subgraph (excluding direct unmerges)
        INSERT /*+ APPEND */ INTO GTT_ACTIVE_EDGES (U_NODE, V_NODE)
        WITH EFFECTIVE_DELTA_PAIRS AS (
            SELECT h.ID1, h.ID2
              FROM PME_MTCH_HIST h
             WHERE EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = h.ID1)
                OR EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = h.ID2)
            UNION
            SELECT LEAST(o.ID1, o.ID2) AS ID1, GREATEST(o.ID1, o.ID2) AS ID2
              FROM PME_MANUAL_OVERRIDES o
             WHERE o.ISMERGE = 1
               AND o.ID1 <> o.ID2
               AND (EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = o.ID1)
                 OR EXISTS (SELECT 1 FROM GTT_IMPACTED_NODES i WHERE i.NODE_ID = o.ID2))
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM GTT_UNMERGE_RULES
        )
        SELECT ID1, ID2 FROM EFFECTIVE_DELTA_PAIRS
        UNION ALL
        SELECT ID2, ID1 FROM EFFECTIVE_DELTA_PAIRS;

        COMMIT;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 3: TRANSITIVE BRIDGE PRUNING & FINAL CLUSTER CONVERGENCE
    ---------------------------------------------------------------------------
    -- Check if there are any active unmerge rules in this run
    DECLARE
        v_unmerge_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_unmerge_cnt FROM GTT_UNMERGE_RULES;

        IF v_unmerge_cnt > 0 THEN
            -- PASS 1: Run label propagation holding out unmerge TARGET_NODEs
            run_label_propagation(p_exclude_targets => TRUE);

            -- Log transitive bridges that are about to be cut (optional audit trail)
            INSERT INTO PME_TRANSITIVE_CUT_LOG (
                UNMERGE_ANCHOR, UNMERGE_TARGET, CUT_NODE_1, CUT_NODE_2, LOGGED_DT
            )
            SELECT DISTINCT
                   r.ANCHOR_NODE,
                   r.TARGET_NODE,
                   LEAST(e.U_NODE, e.V_NODE),
                   GREATEST(e.U_NODE, e.V_NODE),
                   SYSDATE
              FROM GTT_UNMERGE_RULES r
              JOIN GTT_NODE_CLUSTER ca ON ca.NODE_ID = r.ANCHOR_NODE
              JOIN GTT_ACTIVE_EDGES e  ON e.V_NODE   = r.TARGET_NODE
              JOIN GTT_NODE_CLUSTER cw ON cw.NODE_ID = e.U_NODE
             WHERE cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
               -- Do not cut an edge if a steward explicitly forced ISMERGE = 1 on that exact pair
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND LEAST(m.ID1, m.ID2)    = LEAST(e.U_NODE, e.V_NODE)
                      AND GREATEST(m.ID1, m.ID2) = GREATEST(e.U_NODE, e.V_NODE)
               );

            -- Cut both directions (W -> TARGET_NODE and TARGET_NODE -> W) of transitive bridges
            -- where neighbor W belongs to the same Pass-1 component as ANCHOR_NODE
            DELETE FROM GTT_ACTIVE_EDGES e
             WHERE EXISTS (
                   SELECT 1
                     FROM GTT_UNMERGE_RULES r
                     JOIN GTT_NODE_CLUSTER ca ON ca.NODE_ID = r.ANCHOR_NODE
                     JOIN GTT_NODE_CLUSTER cw ON cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
                    WHERE ( (e.U_NODE = cw.NODE_ID AND e.V_NODE = r.TARGET_NODE)
                         OR (e.U_NODE = r.TARGET_NODE AND e.V_NODE = cw.NODE_ID) )
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND LEAST(m.ID1, m.ID2)    = LEAST(e.U_NODE, e.V_NODE)
                      AND GREATEST(m.ID1, m.ID2) = GREATEST(e.U_NODE, e.V_NODE)
               );

            COMMIT;
        END IF;

        -- PASS 2: Final Cluster Convergence on the clean edge set
        run_label_propagation(p_exclude_targets => FALSE);
    END;

    ---------------------------------------------------------------------------
    -- STEP 4: SYNC CLUSTER RESULTS & UPDATE PARTY_XREF
    ---------------------------------------------------------------------------
    DELETE FROM PME_CLUSTER_RESULT
     WHERE NODE_ID IN (SELECT NODE_ID FROM GTT_NODE_CLUSTER);

    INSERT INTO PME_CLUSTER_RESULT (CLUSTER_ROOT, NODE_ID, UPDATED_DT)
    SELECT CLUSTER_ROOT, NODE_ID, SYSDATE
      FROM GTT_NODE_CLUSTER
     WHERE CLUSTER_ROOT <> NODE_ID;

    MERGE INTO PARTY_XREF x
    USING GTT_NODE_CLUSTER c
    ON (x.ORIG_ROWID_OBJECT = c.NODE_ID)
    WHEN MATCHED THEN
        UPDATE SET x.ROWID_OBJECT = c.CLUSTER_ROOT
         WHERE x.ROWID_OBJECT <> c.CLUSTER_ROOT;

    COMMIT;
END PRC_RUN_MDM_MERGE;
/
