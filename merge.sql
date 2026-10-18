------------------------------------------ DDL
-- Daily Match Engine Output Table
CREATE TABLE PME_MTCH (
    ID1           NUMBER       NOT NULL,
    ID2           NUMBER       NOT NULL,
    ID1_PARTY_TYP VARCHAR2(10) NOT NULL,
    ID2_PARTY_TYP VARCHAR2(10) NOT NULL,
    PAIR_TYP      VARCHAR2(10) GENERATED ALWAYS AS (
                      CASE
                          WHEN ID1_PARTY_TYP = ID2_PARTY_TYP THEN ID1_PARTY_TYP
                          ELSE 'CROSS'
                      END
                  ) VIRTUAL
);

-- Daily Parties Sent to PME (For Auto-Unmerge when PME returns 0 matches)
CREATE TABLE PME_SENT_PARTIES (
    PARTY_TYP VARCHAR2(10) NOT NULL,
    NODE_ID   NUMBER       NOT NULL,
    CONSTRAINT PK_PME_SENT_PARTIES PRIMARY KEY (PARTY_TYP, NODE_ID) USING INDEX LOCAL
)
PARTITION BY LIST (PARTY_TYP) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL')
);

-------------------------------------------------------------------------------
-- 2. NORMALIZED PAIR TABLES WITH VIRTUAL COLUMN PARTITIONING
-------------------------------------------------------------------------------
-- Accumulated Match History (160M+ Unique Pairs)
CREATE TABLE PME_MTCH_HIST (
    ID1           NUMBER       NOT NULL,
    ID2           NUMBER       NOT NULL,
    ID1_PARTY_TYP VARCHAR2(10) NOT NULL,
    ID2_PARTY_TYP VARCHAR2(10) NOT NULL,
    CREATED_DT    DATE         DEFAULT SYSDATE,
    PAIR_TYP      VARCHAR2(10) GENERATED ALWAYS AS (
                      CASE
                          WHEN ID1_PARTY_TYP = ID2_PARTY_TYP THEN ID1_PARTY_TYP
                          ELSE 'CROSS'
                      END
                  ) VIRTUAL,
    CONSTRAINT CHK_HIST_ID_ORDER CHECK (ID1 < ID2),
    CONSTRAINT PK_PME_MTCH_HIST PRIMARY KEY (PAIR_TYP, ID1, ID2) USING INDEX LOCAL
)
PARTITION BY LIST (PAIR_TYP) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

-- Manual Overrides Table (Priority over PME_MTCH_HIST)
CREATE TABLE PME_MANUAL_OVERRIDES (
    ID1           NUMBER       NOT NULL,
    ID2           NUMBER       NOT NULL,
    ID1_PARTY_TYP VARCHAR2(10) NOT NULL,
    ID2_PARTY_TYP VARCHAR2(10) NOT NULL,
    ISMERGE       NUMBER(1)    DEFAULT 0 NOT NULL,
    ISUNMERGE     NUMBER(1)    DEFAULT 0 NOT NULL,
    CREATED_DT    DATE         DEFAULT SYSDATE,
    PAIR_TYP      VARCHAR2(10) GENERATED ALWAYS AS (
                      CASE
                          WHEN ID1_PARTY_TYP = ID2_PARTY_TYP THEN ID1_PARTY_TYP
                          ELSE 'CROSS'
                      END
                  ) VIRTUAL,
    CONSTRAINT CHK_OVR_ID_ORDER CHECK (ID1 < ID2),
    CONSTRAINT CHK_OVR_FLAGS    CHECK ((ISMERGE = 1 AND ISUNMERGE = 0) OR (ISMERGE = 0 AND ISUNMERGE = 1)),
    CONSTRAINT PK_PME_OVERRIDES PRIMARY KEY (PAIR_TYP, ID1, ID2) USING INDEX LOCAL
)
PARTITION BY LIST (PAIR_TYP) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

-- Final Cluster Output Table (Partitioned by NODE_PARTY_TYP)
CREATE TABLE PME_CLUSTER_RESULT (
    CLUSTER_ROOT   NUMBER       NOT NULL,
    NODE_ID        NUMBER       NOT NULL,
    ROOT_PARTY_TYP VARCHAR2(10) NOT NULL,
    NODE_PARTY_TYP VARCHAR2(10) NOT NULL,
    IS_CROSS_TYPE  VARCHAR2(1)  GENERATED ALWAYS AS (
                       CASE WHEN ROOT_PARTY_TYP <> NODE_PARTY_TYP THEN 'Y' ELSE 'N' END
                   ) VIRTUAL,
    UPDATED_DT     DATE         DEFAULT SYSDATE,
    CONSTRAINT PK_PME_CLUSTER_RESULT PRIMARY KEY (NODE_PARTY_TYP, NODE_ID) USING INDEX LOCAL
)
PARTITION BY LIST (NODE_PARTY_TYP) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL')
);
CREATE INDEX IDX_PME_CLUST_ROOT ON PME_CLUSTER_RESULT (NODE_PARTY_TYP, CLUSTER_ROOT) LOCAL;

-- Audit Log for Transitive Bridges Broken by Unmerges
CREATE TABLE PME_TRANSITIVE_CUT_LOG (
    WORK_STREAM    VARCHAR2(10) NOT NULL,
    UNMERGE_ANCHOR NUMBER       NOT NULL,
    UNMERGE_TARGET NUMBER       NOT NULL,
    CUT_NODE_1     NUMBER       NOT NULL,
    CUT_NODE_2     NUMBER       NOT NULL,
    LOGGED_DT      DATE         DEFAULT SYSDATE
);

-------------------------------------------------------------------------------
-- 3. PHYSICAL STAGING TABLES (REPLACING GTTs, PARTITIONED BY WORK_STREAM)
-------------------------------------------------------------------------------
-- 3a. Raw Deduplicated Pairs + Degree-1 Fast-Path Flag
CREATE TABLE STG_RAW_PAIRS (
    WORK_STREAM VARCHAR2(10) NOT NULL,
    ID1         NUMBER       NOT NULL,
    ID2         NUMBER       NOT NULL,
    IS_DEGREE_1 NUMBER(1)    NOT NULL
) NOLOGGING
PARTITION BY LIST (WORK_STREAM) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

-- 3b. Active Undirected Graph Edges (Only Complex Clusters: IS_DEGREE_1 = 0)
CREATE TABLE STG_ACTIVE_EDGES (
    WORK_STREAM VARCHAR2(10) NOT NULL,
    U_NODE      NUMBER       NOT NULL,
    V_NODE      NUMBER       NOT NULL
) NOLOGGING
PARTITION BY LIST (WORK_STREAM) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);
CREATE INDEX IDX_STG_EDGES_UV ON STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE) LOCAL NOLOGGING;

-- 3c. Iterative Node -> Cluster Root Convergence Table
CREATE TABLE STG_NODE_CLUSTER (
    WORK_STREAM  VARCHAR2(10) NOT NULL,
    NODE_ID      NUMBER       NOT NULL,
    CLUSTER_ROOT NUMBER       NOT NULL,
    CONSTRAINT PK_STG_NODE_CLUSTER PRIMARY KEY (WORK_STREAM, NODE_ID) USING INDEX LOCAL NOLOGGING
) NOLOGGING
PARTITION BY LIST (WORK_STREAM) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

-- 3d. Impacted Nodes & Dependencies
CREATE TABLE STG_IMPACTED_NODES (
    WORK_STREAM VARCHAR2(10) NOT NULL,
    NODE_ID     NUMBER       NOT NULL,
    CONSTRAINT PK_STG_IMPACTED_NODES PRIMARY KEY (WORK_STREAM, NODE_ID) USING INDEX LOCAL NOLOGGING
) NOLOGGING
PARTITION BY LIST (WORK_STREAM) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

-- 3e. Active Unmerge Rules for the Current Run
CREATE TABLE STG_UNMERGE_RULES (
    WORK_STREAM VARCHAR2(10) NOT NULL,
    ANCHOR_NODE NUMBER       NOT NULL,
    TARGET_NODE NUMBER       NOT NULL,
    CONSTRAINT PK_STG_UNMERGE_RULES PRIMARY KEY (WORK_STREAM, ANCHOR_NODE, TARGET_NODE) USING INDEX LOCAL NOLOGGING
) NOLOGGING
PARTITION BY LIST (WORK_STREAM) (
    PARTITION P_BSNS  VALUES ('BSNS'),
    PARTITION P_INDVL VALUES ('INDVL'),
    PARTITION P_CROSS VALUES ('CROSS')
);

---------------------------------

CREATE OR REPLACE TRIGGER TRG_BIU_PME_MANUAL_OVERRIDES
BEFORE INSERT OR UPDATE OF ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP
ON PME_MANUAL_OVERRIDES
FOR EACH ROW
DECLARE
    v_tmp_id   NUMBER;
    v_tmp_type VARCHAR2(10);
BEGIN
    -- Look up ID1_PARTY_TYP from PARTY_XREF if not provided
    IF :NEW.ID1_PARTY_TYP IS NULL THEN
        SELECT PARTY_TYP INTO :NEW.ID1_PARTY_TYP
          FROM PARTY_XREF
         WHERE ORIG_ROWID_OBJECT = :NEW.ID1;
    END IF;

    -- Look up ID2_PARTY_TYP from PARTY_XREF if not provided
    IF :NEW.ID2_PARTY_TYP IS NULL THEN
        SELECT PARTY_TYP INTO :NEW.ID2_PARTY_TYP
          FROM PARTY_XREF
         WHERE ORIG_ROWID_OBJECT = :NEW.ID2;
    END IF;

    -- Normalize so ID1 < ID2 and keep PARTY_TYP aligned with its ID
    IF :NEW.ID1 > :NEW.ID2 THEN
        v_tmp_id           := :NEW.ID1;
        v_tmp_type         := :NEW.ID1_PARTY_TYP;

        :NEW.ID1           := :NEW.ID2;
        :NEW.ID1_PARTY_TYP := :NEW.ID2_PARTY_TYP;

        :NEW.ID2           := v_tmp_id;
        :NEW.ID2_PARTY_TYP := v_tmp_type;
    END IF;
END;
/

------------------------------------------------ Procs

CREATE OR REPLACE PROCEDURE PRC_RUN_MDM_MERGE_BY_TYPE (
    p_party_typ IN VARCHAR2,                 -- 'BSNS' or 'INDVL'
    p_run_mode  IN VARCHAR2 DEFAULT 'DELTA'  -- 'DAY1' or 'DELTA'
) AS
    v_part_name VARCHAR2(30) := 'P_' || UPPER(p_party_typ);

    -- Truncate only this work stream's partition
    PROCEDURE truncate_stream_partition (p_table_name IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name ||
                          ' TRUNCATE PARTITION ' || v_part_name || ' UPDATE INDEXES';
    END truncate_stream_partition;

    -- Min-Label Propagation + Pointer Jumping
    PROCEDURE run_label_propagation (p_exclude_targets IN BOOLEAN) IS
        v_rows_updated NUMBER := 1;
    BEGIN
        truncate_stream_partition('STG_NODE_CLUSTER');

        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_NODE_CLUSTER (WORK_STREAM, NODE_ID, CLUSTER_ROOT)
        SELECT p_party_typ, NODE_ID, NODE_ID
          FROM STG_IMPACTED_NODES
         WHERE WORK_STREAM = p_party_typ;
        COMMIT;

        WHILE v_rows_updated > 0 LOOP
            IF p_exclude_targets THEN
                MERGE /*+ PARALLEL(4) */ INTO STG_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM STG_ACTIVE_EDGES e
                      JOIN STG_NODE_CLUSTER nc
                        ON e.V_NODE = nc.NODE_ID
                       AND nc.WORK_STREAM = p_party_typ
                     WHERE e.WORK_STREAM = p_party_typ
                       AND NOT EXISTS (
                           SELECT 1 FROM STG_UNMERGE_RULES r
                            WHERE r.WORK_STREAM = p_party_typ
                              AND (r.TARGET_NODE = e.U_NODE OR r.TARGET_NODE = e.V_NODE)
                       )
                     GROUP BY e.U_NODE
                ) src
                ON (c.WORK_STREAM = p_party_typ AND c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            ELSE
                MERGE /*+ PARALLEL(4) */ INTO STG_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM STG_ACTIVE_EDGES e
                      JOIN STG_NODE_CLUSTER nc
                        ON e.V_NODE = nc.NODE_ID
                       AND nc.WORK_STREAM = p_party_typ
                     WHERE e.WORK_STREAM = p_party_typ
                     GROUP BY e.U_NODE
                ) src
                ON (c.WORK_STREAM = p_party_typ AND c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            END IF;

            v_rows_updated := SQL%ROWCOUNT;
            COMMIT;

            -- Pointer Jumping (Root Compression)
            MERGE /*+ PARALLEL(4) */ INTO STG_NODE_CLUSTER c
            USING (
                SELECT NODE_ID, CLUSTER_ROOT
                  FROM STG_NODE_CLUSTER
                 WHERE WORK_STREAM = p_party_typ
            ) p
            ON (c.WORK_STREAM = p_party_typ AND c.CLUSTER_ROOT = p.NODE_ID)
            WHEN MATCHED THEN
                UPDATE SET c.CLUSTER_ROOT = p.CLUSTER_ROOT
                 WHERE p.CLUSTER_ROOT < c.CLUSTER_ROOT;

            v_rows_updated := v_rows_updated + SQL%ROWCOUNT;
            COMMIT;
        END LOOP;
    END run_label_propagation;

BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- Clear ONLY this stream's partitions
    truncate_stream_partition('STG_RAW_PAIRS');
    truncate_stream_partition('STG_ACTIVE_EDGES');
    truncate_stream_partition('STG_NODE_CLUSTER');
    truncate_stream_partition('STG_IMPACTED_NODES');
    truncate_stream_partition('STG_UNMERGE_RULES');

    ---------------------------------------------------------------------------
    -- STEP 1: SEED / REFRESH PME_MTCH_HIST & CAPTURE DELTA DEPENDENCIES
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        -- Seed Day 1 same-type pairs from PARTY_XREF
        MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
        USING (
            SELECT DISTINCT
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x1.ROWID_OBJECT      ELSE x1.ORIG_ROWID_OBJECT END AS ID1,
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x1.ORIG_ROWID_OBJECT ELSE x1.ROWID_OBJECT      END AS ID2,
                   p_party_typ AS ID1_PARTY_TYP,
                   p_party_typ AS ID2_PARTY_TYP,
                   p_party_typ AS CALC_PAIR_TYP
              FROM PARTY_XREF x1
              JOIN PARTY_XREF x2 ON x1.ROWID_OBJECT = x2.ORIG_ROWID_OBJECT
             WHERE x1.ROWID_OBJECT <> x1.ORIG_ROWID_OBJECT
               AND x1.PARTY_TYP = p_party_typ
               AND x2.PARTY_TYP = p_party_typ
        ) x
        ON (h.PAIR_TYP = x.CALC_PAIR_TYP AND h.ID1 = x.ID1 AND h.ID2 = x.ID2)
        WHEN NOT MATCHED THEN
            INSERT (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, CREATED_DT)
            VALUES (x.ID1, x.ID2, x.ID1_PARTY_TYP, x.ID2_PARTY_TYP, SYSDATE);
        COMMIT;

    ELSE
        -- DELTA MODE:
        -- 1A. Seed impacted nodes BEFORE deleting stale PME_MTCH_HIST edges for PME_SENT_PARTIES
        INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
        SELECT p_party_typ, NODE_ID
          FROM (
            SELECT ID1 AS NODE_ID FROM PME_MTCH WHERE PAIR_TYP = p_party_typ AND ID1 <> ID2
            UNION
            SELECT ID2 FROM PME_MTCH WHERE PAIR_TYP = p_party_typ AND ID1 <> ID2
            UNION
            SELECT ID1 FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = p_party_typ AND (ISMERGE = 1 OR ISUNMERGE = 1)
            UNION
            SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = p_party_typ AND (ISMERGE = 1 OR ISUNMERGE = 1)
            UNION
            SELECT s.NODE_ID
              FROM PME_SENT_PARTIES s
             WHERE s.PARTY_TYP = p_party_typ
               AND (
                   EXISTS (
                       SELECT 1 FROM PARTY_XREF x
                        WHERE x.PARTY_TYP = p_party_typ
                          AND (x.ORIG_ROWID_OBJECT = s.NODE_ID OR x.ROWID_OBJECT = s.NODE_ID)
                          AND x.ROWID_OBJECT <> x.ORIG_ROWID_OBJECT
                   )
                   OR EXISTS (
                       SELECT 1 FROM PME_MTCH_HIST h
                        WHERE h.PAIR_TYP = p_party_typ
                          AND (h.ID1 = s.NODE_ID OR h.ID2 = s.NODE_ID)
                   )
               )
          );
        COMMIT;

        -- 1B. Expand existing cluster members & prior history neighbors before purging stale edges
        LOOP
            INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
            WITH NEW_DEPS AS (
                SELECT x.ORIG_ROWID_OBJECT AS DEP_NODE
                  FROM PARTY_XREF x
                 WHERE x.PARTY_TYP = p_party_typ
                   AND x.ROWID_OBJECT IN (
                       SELECT p.ROWID_OBJECT
                         FROM PARTY_XREF p
                         JOIN STG_IMPACTED_NODES i
                           ON p.ORIG_ROWID_OBJECT = i.NODE_ID AND i.WORK_STREAM = p_party_typ
                        WHERE p.PARTY_TYP = p_party_typ
                       UNION
                       SELECT NODE_ID FROM STG_IMPACTED_NODES WHERE WORK_STREAM = p_party_typ
                   )
                UNION
                SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END AS DEP_NODE
                  FROM PME_MTCH_HIST h
                  JOIN STG_IMPACTED_NODES i
                    ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID) AND i.WORK_STREAM = p_party_typ
                 WHERE h.PAIR_TYP = p_party_typ
            )
            SELECT DISTINCT p_party_typ, d.DEP_NODE
              FROM NEW_DEPS d
             WHERE NOT EXISTS (
                   SELECT 1 FROM STG_IMPACTED_NODES e
                    WHERE e.WORK_STREAM = p_party_typ AND e.NODE_ID = d.DEP_NODE
             );

            EXIT WHEN SQL%ROWCOUNT = 0;
            COMMIT;
        END LOOP;

        -- 1C. Purge stale automated matches in PME_MTCH_HIST for parties re-evaluated by PME today
        DELETE /*+ PARALLEL(4) */ FROM PME_MTCH_HIST h
         WHERE h.PAIR_TYP = p_party_typ
           AND (EXISTS (SELECT 1 FROM PME_SENT_PARTIES s WHERE s.PARTY_TYP = p_party_typ AND s.NODE_ID = h.ID1)
             OR EXISTS (SELECT 1 FROM PME_SENT_PARTIES s WHERE s.PARTY_TYP = p_party_typ AND s.NODE_ID = h.ID2));
        COMMIT;
    END IF;

    -- 1D. Merge today's PME_MTCH into PME_MTCH_HIST (Normalized with corresponding PARTY_TYP)
    MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
    USING (
        SELECT DISTINCT
               CASE WHEN m.ID1 < m.ID2 THEN m.ID1           ELSE m.ID2           END AS ID1,
               CASE WHEN m.ID1 < m.ID2 THEN m.ID2           ELSE m.ID1           END AS ID2,
               CASE WHEN m.ID1 < m.ID2 THEN m.ID1_PARTY_TYP ELSE m.ID2_PARTY_TYP END AS ID1_PARTY_TYP,
               CASE WHEN m.ID1 < m.ID2 THEN m.ID2_PARTY_TYP ELSE m.ID1_PARTY_TYP END AS ID2_PARTY_TYP,
               m.PAIR_TYP
          FROM PME_MTCH m
         WHERE m.PAIR_TYP = p_party_typ
           AND m.ID1 <> m.ID2
    ) src
    ON (h.PAIR_TYP = src.PAIR_TYP AND h.ID1 = src.ID1 AND h.ID2 = src.ID2)
    WHEN NOT MATCHED THEN
        INSERT (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, CREATED_DT)
        VALUES (src.ID1, src.ID2, src.ID1_PARTY_TYP, src.ID2_PARTY_TYP, SYSDATE);
    COMMIT;

    ---------------------------------------------------------------------------
    -- STEP 2: BUILD EFFECTIVE PAIRS & TAG DEGREE-1 ISOLATED PAIRS
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT p_party_typ, ID1, ID2
          FROM PME_MANUAL_OVERRIDES
         WHERE PAIR_TYP  = p_party_typ
           AND ISUNMERGE = 1;
        COMMIT;

        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_RAW_PAIRS (WORK_STREAM, ID1, ID2, IS_DEGREE_1)
        WITH EFFECTIVE_PAIRS AS (
            SELECT ID1, ID2
              FROM PME_MTCH_HIST
             WHERE PAIR_TYP = p_party_typ
            UNION
            SELECT ID1, ID2
              FROM PME_MANUAL_OVERRIDES
             WHERE PAIR_TYP = p_party_typ
               AND ISMERGE  = 1
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM STG_UNMERGE_RULES
             WHERE WORK_STREAM = p_party_typ
        ),
        NODE_DEGREES AS (
            SELECT NODE_ID, COUNT(*) AS DEG
              FROM (
                SELECT ID1 AS NODE_ID FROM EFFECTIVE_PAIRS
                UNION ALL
                SELECT ID2 AS NODE_ID FROM EFFECTIVE_PAIRS
              )
             GROUP BY NODE_ID
        )
        SELECT p_party_typ,
               ep.ID1,
               ep.ID2,
               CASE
                   WHEN d1.DEG = 1 AND d2.DEG = 1
                    AND NOT EXISTS (
                        SELECT 1 FROM STG_UNMERGE_RULES r
                         WHERE r.WORK_STREAM = p_party_typ
                           AND (r.ANCHOR_NODE IN (ep.ID1, ep.ID2) OR r.TARGET_NODE IN (ep.ID1, ep.ID2))
                    )
                   THEN 1
                   ELSE 0
               END AS IS_DEGREE_1
          FROM EFFECTIVE_PAIRS ep
          JOIN NODE_DEGREES d1 ON d1.NODE_ID = ep.ID1
          JOIN NODE_DEGREES d2 ON d2.NODE_ID = ep.ID2;
        COMMIT;

    ELSE
        -- DELTA MODE: Expand any new neighbors introduced by today's PME_MTCH
        LOOP
            INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
            WITH NEW_DEPS AS (
                SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END AS DEP_NODE
                  FROM PME_MTCH_HIST h
                  JOIN STG_IMPACTED_NODES i
                    ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID) AND i.WORK_STREAM = p_party_typ
                 WHERE h.PAIR_TYP = p_party_typ
                UNION
                SELECT x.ORIG_ROWID_OBJECT AS DEP_NODE
                  FROM PARTY_XREF x
                 WHERE x.PARTY_TYP = p_party_typ
                   AND x.ROWID_OBJECT IN (
                       SELECT p.ROWID_OBJECT
                         FROM PARTY_XREF p
                         JOIN STG_IMPACTED_NODES i
                           ON p.ORIG_ROWID_OBJECT = i.NODE_ID AND i.WORK_STREAM = p_party_typ
                        WHERE p.PARTY_TYP = p_party_typ
                   )
            )
            SELECT DISTINCT p_party_typ, d.DEP_NODE
              FROM NEW_DEPS d
             WHERE NOT EXISTS (
                   SELECT 1 FROM STG_IMPACTED_NODES e
                    WHERE e.WORK_STREAM = p_party_typ AND e.NODE_ID = d.DEP_NODE
             );

            EXIT WHEN SQL%ROWCOUNT = 0;
            COMMIT;
        END LOOP;

        INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT p_party_typ, o.ID1, o.ID2
          FROM PME_MANUAL_OVERRIDES o
         WHERE o.PAIR_TYP  = p_party_typ
           AND o.ISUNMERGE = 1
           AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID1)
             OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID2));
        COMMIT;

        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_RAW_PAIRS (WORK_STREAM, ID1, ID2, IS_DEGREE_1)
        WITH EFFECTIVE_DELTA_PAIRS AS (
            SELECT h.ID1, h.ID2
              FROM PME_MTCH_HIST h
             WHERE h.PAIR_TYP = p_party_typ
               AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = h.ID1)
                 OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = h.ID2))
            UNION
            SELECT o.ID1, o.ID2
              FROM PME_MANUAL_OVERRIDES o
             WHERE o.PAIR_TYP = p_party_typ
               AND o.ISMERGE  = 1
               AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID1)
                 OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID2))
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM STG_UNMERGE_RULES
             WHERE WORK_STREAM = p_party_typ
        ),
        NODE_DEGREES AS (
            SELECT NODE_ID, COUNT(*) AS DEG
              FROM (
                SELECT ID1 AS NODE_ID FROM EFFECTIVE_DELTA_PAIRS
                UNION ALL
                SELECT ID2 AS NODE_ID FROM EFFECTIVE_DELTA_PAIRS
              )
             GROUP BY NODE_ID
        )
        SELECT p_party_typ,
               ep.ID1,
               ep.ID2,
               CASE
                   WHEN d1.DEG = 1 AND d2.DEG = 1
                    AND NOT EXISTS (
                        SELECT 1 FROM STG_UNMERGE_RULES r
                         WHERE r.WORK_STREAM = p_party_typ
                           AND (r.ANCHOR_NODE IN (ep.ID1, ep.ID2) OR r.TARGET_NODE IN (ep.ID1, ep.ID2))
                    )
                   THEN 1
                   ELSE 0
               END AS IS_DEGREE_1
          FROM EFFECTIVE_DELTA_PAIRS ep
          JOIN NODE_DEGREES d1 ON d1.NODE_ID = ep.ID1
          JOIN NODE_DEGREES d2 ON d2.NODE_ID = ep.ID2;
        COMMIT;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 2B: POPULATE STG_ACTIVE_EDGES ONLY FOR COMPLEX CLUSTERS (IS_DEGREE_1 = 0)
    ---------------------------------------------------------------------------
    INSERT /*+ APPEND PARALLEL(4) */ INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
    SELECT p_party_typ, ID1, ID2
      FROM STG_RAW_PAIRS
     WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0
    UNION ALL
    SELECT p_party_typ, ID2, ID1
      FROM STG_RAW_PAIRS
     WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0;
    COMMIT;

    IF UPPER(p_run_mode) = 'DAY1' THEN
        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
        SELECT p_party_typ, NODE_ID
          FROM (
            SELECT U_NODE AS NODE_ID FROM STG_ACTIVE_EDGES WHERE WORK_STREAM = p_party_typ
            UNION
            SELECT ANCHOR_NODE FROM STG_UNMERGE_RULES WHERE WORK_STREAM = p_party_typ
            UNION
            SELECT TARGET_NODE FROM STG_UNMERGE_RULES WHERE WORK_STREAM = p_party_typ
            UNION
            SELECT ORIG_ROWID_OBJECT
              FROM PARTY_XREF
             WHERE PARTY_TYP = p_party_typ
               AND ROWID_OBJECT <> ORIG_ROWID_OBJECT
            MINUS
            SELECT ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
            MINUS
            SELECT ID2 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
          );
        COMMIT;
    ELSE
        -- Remove Degree-1 fast-path nodes from STG_IMPACTED_NODES while keeping
        -- all complex nodes AND 0-edge auto-unmerged nodes
        DELETE FROM STG_IMPACTED_NODES i
         WHERE i.WORK_STREAM = p_party_typ
           AND EXISTS (
               SELECT 1 FROM STG_RAW_PAIRS r
                WHERE r.WORK_STREAM = p_party_typ
                  AND r.IS_DEGREE_1 = 1
                  AND (r.ID1 = i.NODE_ID OR r.ID2 = i.NODE_ID)
           );
        COMMIT;
    END IF;

    DBMS_STATS.GATHER_TABLE_STATS(USER, 'STG_ACTIVE_EDGES',   PARTNAME => v_part_name, GRANULARITY => 'PARTITION');
    DBMS_STATS.GATHER_TABLE_STATS(USER, 'STG_IMPACTED_NODES', PARTNAME => v_part_name, GRANULARITY => 'PARTITION');

    ---------------------------------------------------------------------------
    -- STEP 3: 2-PASS TRANSITIVE BRIDGE CUTTING + CONVERGENCE + DEGREE-1 APPEND
    ---------------------------------------------------------------------------
    DECLARE
        v_unmerge_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_unmerge_cnt
          FROM STG_UNMERGE_RULES
         WHERE WORK_STREAM = p_party_typ;

        IF v_unmerge_cnt > 0 THEN
            -- Pass 1: Holdout unmerge target nodes
            run_label_propagation(p_exclude_targets => TRUE);

            -- Audit log transitive bridges being severed
            INSERT INTO PME_TRANSITIVE_CUT_LOG (
                WORK_STREAM, UNMERGE_ANCHOR, UNMERGE_TARGET, CUT_NODE_1, CUT_NODE_2, LOGGED_DT
            )
            SELECT DISTINCT
                   p_party_typ,
                   r.ANCHOR_NODE,
                   r.TARGET_NODE,
                   LEAST(e.U_NODE, e.V_NODE),
                   GREATEST(e.U_NODE, e.V_NODE),
                   SYSDATE
              FROM STG_UNMERGE_RULES r
              JOIN STG_NODE_CLUSTER ca ON ca.WORK_STREAM = p_party_typ AND ca.NODE_ID = r.ANCHOR_NODE
              JOIN STG_ACTIVE_EDGES e  ON e.WORK_STREAM  = p_party_typ AND e.V_NODE   = r.TARGET_NODE
              JOIN STG_NODE_CLUSTER cw ON cw.WORK_STREAM = p_party_typ AND cw.NODE_ID = e.U_NODE
             WHERE r.WORK_STREAM   = p_party_typ
               AND cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.PAIR_TYP = p_party_typ
                      AND m.ISMERGE  = 1
                      AND m.ID1      = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2      = GREATEST(e.U_NODE, e.V_NODE)
               );

            -- Sever transitive bridges
            DELETE FROM STG_ACTIVE_EDGES e
             WHERE e.WORK_STREAM = p_party_typ
               AND EXISTS (
                   SELECT 1
                     FROM STG_UNMERGE_RULES r
                     JOIN STG_NODE_CLUSTER ca
                       ON ca.WORK_STREAM = p_party_typ AND ca.NODE_ID = r.ANCHOR_NODE
                     JOIN STG_NODE_CLUSTER cw
                       ON cw.WORK_STREAM = p_party_typ AND cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
                    WHERE r.WORK_STREAM = p_party_typ
                      AND ( (e.U_NODE = cw.NODE_ID AND e.V_NODE = r.TARGET_NODE)
                         OR (e.U_NODE = r.TARGET_NODE AND e.V_NODE = cw.NODE_ID) )
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.PAIR_TYP = p_party_typ
                      AND m.ISMERGE  = 1
                      AND m.ID1      = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2      = GREATEST(e.U_NODE, e.V_NODE)
               );
            COMMIT;
        END IF;

        -- Pass 2: Final Convergence
        run_label_propagation(p_exclude_targets => FALSE);
    END;

    -- 3b. Append pre-solved Degree-1 pairs directly into STG_NODE_CLUSTER
    INSERT /*+ APPEND PARALLEL(4) */ INTO STG_NODE_CLUSTER (WORK_STREAM, NODE_ID, CLUSTER_ROOT)
    SELECT p_party_typ, ID1, ID1
      FROM STG_RAW_PAIRS
     WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
    UNION ALL
    SELECT p_party_typ, ID2, ID1
      FROM STG_RAW_PAIRS
     WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1;
    COMMIT;

    ---------------------------------------------------------------------------
    -- STEP 4: SYNC PME_CLUSTER_RESULT & UPDATE PARTY_XREF
    ---------------------------------------------------------------------------
    DELETE FROM PME_CLUSTER_RESULT
     WHERE NODE_PARTY_TYP = p_party_typ
       AND NODE_ID IN (
           SELECT NODE_ID FROM STG_NODE_CLUSTER WHERE WORK_STREAM = p_party_typ
       );

    INSERT /*+ APPEND */ INTO PME_CLUSTER_RESULT (
        CLUSTER_ROOT, NODE_ID, ROOT_PARTY_TYP, NODE_PARTY_TYP, UPDATED_DT
    )
    SELECT CLUSTER_ROOT, NODE_ID, p_party_typ, p_party_typ, SYSDATE
      FROM STG_NODE_CLUSTER
     WHERE WORK_STREAM = p_party_typ
       AND CLUSTER_ROOT <> NODE_ID;
    COMMIT;

    MERGE /*+ PARALLEL(4) */ INTO PARTY_XREF x
    USING (
        SELECT NODE_ID, CLUSTER_ROOT
          FROM STG_NODE_CLUSTER
         WHERE WORK_STREAM = p_party_typ
    ) c
    ON (x.ORIG_ROWID_OBJECT = c.NODE_ID AND x.PARTY_TYP = p_party_typ)
    WHEN MATCHED THEN
        UPDATE SET x.ROWID_OBJECT = c.CLUSTER_ROOT
         WHERE x.ROWID_OBJECT <> c.CLUSTER_ROOT;

    COMMIT;
END PRC_RUN_MDM_MERGE_BY_TYPE;
/
--------------------------

CREATE OR REPLACE PROCEDURE PRC_RUN_CROSS_TYPE_MERGE (
    p_run_mode IN VARCHAR2 DEFAULT 'DELTA' -- 'DAY1' or 'DELTA'
) AS
    v_cross_cnt NUMBER := 0;

    PROCEDURE truncate_cross_partition (p_table_name IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name ||
                          ' TRUNCATE PARTITION P_CROSS UPDATE INDEXES';
    END truncate_cross_partition;

    PROCEDURE run_label_propagation (p_exclude_targets IN BOOLEAN) IS
        v_rows_updated NUMBER := 1;
    BEGIN
        truncate_cross_partition('STG_NODE_CLUSTER');

        INSERT INTO STG_NODE_CLUSTER (WORK_STREAM, NODE_ID, CLUSTER_ROOT)
        SELECT 'CROSS', NODE_ID, NODE_ID
          FROM STG_IMPACTED_NODES
         WHERE WORK_STREAM = 'CROSS';
        COMMIT;

        WHILE v_rows_updated > 0 LOOP
            IF p_exclude_targets THEN
                MERGE INTO STG_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM STG_ACTIVE_EDGES e
                      JOIN STG_NODE_CLUSTER nc
                        ON e.V_NODE = nc.NODE_ID
                       AND nc.WORK_STREAM = 'CROSS'
                     WHERE e.WORK_STREAM = 'CROSS'
                       AND NOT EXISTS (
                           SELECT 1 FROM STG_UNMERGE_RULES r
                            WHERE r.WORK_STREAM = 'CROSS'
                              AND (r.TARGET_NODE = e.U_NODE OR r.TARGET_NODE = e.V_NODE)
                       )
                     GROUP BY e.U_NODE
                ) src
                ON (c.WORK_STREAM = 'CROSS' AND c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            ELSE
                MERGE INTO STG_NODE_CLUSTER c
                USING (
                    SELECT e.U_NODE AS NODE_ID,
                           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT
                      FROM STG_ACTIVE_EDGES e
                      JOIN STG_NODE_CLUSTER nc
                        ON e.V_NODE = nc.NODE_ID
                       AND nc.WORK_STREAM = 'CROSS'
                     WHERE e.WORK_STREAM = 'CROSS'
                     GROUP BY e.U_NODE
                ) src
                ON (c.WORK_STREAM = 'CROSS' AND c.NODE_ID = src.NODE_ID)
                WHEN MATCHED THEN
                    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT
                     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT;
            END IF;

            v_rows_updated := SQL%ROWCOUNT;
            COMMIT;

            MERGE INTO STG_NODE_CLUSTER c
            USING (
                SELECT NODE_ID, CLUSTER_ROOT
                  FROM STG_NODE_CLUSTER
                 WHERE WORK_STREAM = 'CROSS'
            ) p
            ON (c.WORK_STREAM = 'CROSS' AND c.CLUSTER_ROOT = p.NODE_ID)
            WHEN MATCHED THEN
                UPDATE SET c.CLUSTER_ROOT = p.CLUSTER_ROOT
                 WHERE p.CLUSTER_ROOT < c.CLUSTER_ROOT;

            v_rows_updated := v_rows_updated + SQL%ROWCOUNT;
            COMMIT;
        END LOOP;
    END run_label_propagation;

BEGIN
    -- Clear P_CROSS partitions
    truncate_cross_partition('STG_RAW_PAIRS');
    truncate_cross_partition('STG_ACTIVE_EDGES');
    truncate_cross_partition('STG_NODE_CLUSTER');
    truncate_cross_partition('STG_IMPACTED_NODES');
    truncate_cross_partition('STG_UNMERGE_RULES');

    ---------------------------------------------------------------------------
    -- STEP 1: SEED HISTORICAL CROSS-TYPE PAIRS FROM PARTY_XREF ON DAY 1
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        MERGE INTO PME_MTCH_HIST h
        USING (
            SELECT DISTINCT
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x1.ROWID_OBJECT      ELSE x1.ORIG_ROWID_OBJECT END AS ID1,
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x1.ORIG_ROWID_OBJECT ELSE x1.ROWID_OBJECT      END AS ID2,
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x2.PARTY_TYP         ELSE x1.PARTY_TYP         END AS ID1_PARTY_TYP,
                   CASE WHEN x1.ROWID_OBJECT < x1.ORIG_ROWID_OBJECT
                        THEN x1.PARTY_TYP         ELSE x2.PARTY_TYP         END AS ID2_PARTY_TYP,
                   'CROSS' AS CALC_PAIR_TYP
              FROM PARTY_XREF x1
              JOIN PARTY_XREF x2 ON x1.ROWID_OBJECT = x2.ORIG_ROWID_OBJECT
             WHERE x1.ROWID_OBJECT <> x1.ORIG_ROWID_OBJECT
               AND x1.PARTY_TYP <> x2.PARTY_TYP
        ) x
        ON (h.PAIR_TYP = x.CALC_PAIR_TYP AND h.ID1 = x.ID1 AND h.ID2 = x.ID2)
        WHEN NOT MATCHED THEN
            INSERT (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, CREATED_DT)
            VALUES (x.ID1, x.ID2, x.ID1_PARTY_TYP, x.ID2_PARTY_TYP, SYSDATE);
        COMMIT;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 2: SEED IMPACTED CROSS-TYPE NODES INTO PARTITION P_CROSS
    ---------------------------------------------------------------------------
    INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
    SELECT 'CROSS', NODE_ID
      FROM (
        SELECT ID1 AS NODE_ID FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
        UNION
        SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
        UNION
        SELECT ID1 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS'
        UNION
        SELECT ID2 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS'
      );

    v_cross_cnt := SQL%ROWCOUNT;
    COMMIT;

    IF v_cross_cnt = 0 THEN
        RETURN;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 3: EXPAND FULL BSNS + INDVL CLUSTERS CONNECTED TO THOSE CROSS NODES
    ---------------------------------------------------------------------------
    LOOP
        INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
        WITH NEW_DEPS AS (
            SELECT r.NODE_ID AS DEP_NODE
              FROM PME_CLUSTER_RESULT r
             WHERE r.CLUSTER_ROOT IN (
                   SELECT r2.CLUSTER_ROOT FROM PME_CLUSTER_RESULT r2
                     JOIN STG_IMPACTED_NODES i
                       ON r2.NODE_ID = i.NODE_ID AND i.WORK_STREAM = 'CROSS'
                   UNION
                   SELECT NODE_ID FROM STG_IMPACTED_NODES WHERE WORK_STREAM = 'CROSS'
             )
            UNION
            SELECT r.CLUSTER_ROOT AS DEP_NODE
              FROM PME_CLUSTER_RESULT r
              JOIN STG_IMPACTED_NODES i
                ON r.NODE_ID = i.NODE_ID AND i.WORK_STREAM = 'CROSS'
            UNION
            SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END
              FROM PME_MTCH_HIST h
              JOIN STG_IMPACTED_NODES i
                ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID) AND i.WORK_STREAM = 'CROSS'
            UNION
            SELECT CASE WHEN o.ID1 = i.NODE_ID THEN o.ID2 ELSE o.ID1 END
              FROM PME_MANUAL_OVERRIDES o
              JOIN STG_IMPACTED_NODES i
                ON (o.ID1 = i.NODE_ID OR o.ID2 = i.NODE_ID) AND i.WORK_STREAM = 'CROSS'
             WHERE o.ISMERGE = 1
        )
        SELECT DISTINCT 'CROSS', d.DEP_NODE
          FROM NEW_DEPS d
         WHERE NOT EXISTS (
               SELECT 1 FROM STG_IMPACTED_NODES e
                WHERE e.WORK_STREAM = 'CROSS' AND e.NODE_ID = d.DEP_NODE
         );

        EXIT WHEN SQL%ROWCOUNT = 0;
        COMMIT;
    END LOOP;

    ---------------------------------------------------------------------------
    -- STEP 4: LOAD UNMERGE RULES & ACTIVE EDGES INTO PARTITION P_CROSS
    ---------------------------------------------------------------------------
    INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
    SELECT DISTINCT 'CROSS', o.ID1, o.ID2
      FROM PME_MANUAL_OVERRIDES o
     WHERE o.ISUNMERGE = 1
       AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID1)
         OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID2));
    COMMIT;

    INSERT INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
    WITH EFFECTIVE_CROSS_PAIRS AS (
        SELECT h.ID1, h.ID2
          FROM PME_MTCH_HIST h
         WHERE EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = h.ID1)
           AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = h.ID2)
        UNION
        SELECT o.ID1, o.ID2
          FROM PME_MANUAL_OVERRIDES o
         WHERE o.ISMERGE = 1
           AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID1)
           AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID2)
        MINUS
        SELECT ANCHOR_NODE, TARGET_NODE
          FROM STG_UNMERGE_RULES
         WHERE WORK_STREAM = 'CROSS'
    )
    SELECT 'CROSS', ID1, ID2 FROM EFFECTIVE_CROSS_PAIRS
    UNION ALL
    SELECT 'CROSS', ID2, ID1 FROM EFFECTIVE_CROSS_PAIRS;
    COMMIT;

    ---------------------------------------------------------------------------
    -- STEP 5: 2-PASS TRANSITIVE BRIDGE CUTTING & FINAL CONVERGENCE
    ---------------------------------------------------------------------------
    DECLARE
        v_unmerge_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_unmerge_cnt
          FROM STG_UNMERGE_RULES
         WHERE WORK_STREAM = 'CROSS';

        IF v_unmerge_cnt > 0 THEN
            run_label_propagation(p_exclude_targets => TRUE);

            INSERT INTO PME_TRANSITIVE_CUT_LOG (
                WORK_STREAM, UNMERGE_ANCHOR, UNMERGE_TARGET, CUT_NODE_1, CUT_NODE_2, LOGGED_DT
            )
            SELECT DISTINCT
                   'CROSS',
                   r.ANCHOR_NODE,
                   r.TARGET_NODE,
                   LEAST(e.U_NODE, e.V_NODE),
                   GREATEST(e.U_NODE, e.V_NODE),
                   SYSDATE
              FROM STG_UNMERGE_RULES r
              JOIN STG_NODE_CLUSTER ca ON ca.WORK_STREAM = 'CROSS' AND ca.NODE_ID = r.ANCHOR_NODE
              JOIN STG_ACTIVE_EDGES e  ON e.WORK_STREAM  = 'CROSS' AND e.V_NODE   = r.TARGET_NODE
              JOIN STG_NODE_CLUSTER cw ON cw.WORK_STREAM = 'CROSS' AND cw.NODE_ID = e.U_NODE
             WHERE r.WORK_STREAM   = 'CROSS'
               AND cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND m.ID1     = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE)
               );

            DELETE FROM STG_ACTIVE_EDGES e
             WHERE e.WORK_STREAM = 'CROSS'
               AND EXISTS (
                   SELECT 1
                     FROM STG_UNMERGE_RULES r
                     JOIN STG_NODE_CLUSTER ca
                       ON ca.WORK_STREAM = 'CROSS' AND ca.NODE_ID = r.ANCHOR_NODE
                     JOIN STG_NODE_CLUSTER cw
                       ON cw.WORK_STREAM = 'CROSS' AND cw.CLUSTER_ROOT = ca.CLUSTER_ROOT
                    WHERE r.WORK_STREAM = 'CROSS'
                      AND ( (e.U_NODE = cw.NODE_ID AND e.V_NODE = r.TARGET_NODE)
                         OR (e.U_NODE = r.TARGET_NODE AND e.V_NODE = cw.NODE_ID) )
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND m.ID1     = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE)
               );
            COMMIT;
        END IF;

        run_label_propagation(p_exclude_targets => FALSE);
    END;

    ---------------------------------------------------------------------------
    -- STEP 6: UPDATE PME_CLUSTER_RESULT & PARTY_XREF
    ---------------------------------------------------------------------------
    DELETE FROM PME_CLUSTER_RESULT
     WHERE NODE_ID IN (
         SELECT NODE_ID FROM STG_NODE_CLUSTER WHERE WORK_STREAM = 'CROSS'
     );

    INSERT INTO PME_CLUSTER_RESULT (
        CLUSTER_ROOT, NODE_ID, ROOT_PARTY_TYP, NODE_PARTY_TYP, UPDATED_DT
    )
    SELECT c.CLUSTER_ROOT,
           c.NODE_ID,
           xr.PARTY_TYP AS ROOT_PARTY_TYP,
           xn.PARTY_TYP AS NODE_PARTY_TYP,
           SYSDATE
      FROM STG_NODE_CLUSTER c
      JOIN PARTY_XREF xn ON xn.ORIG_ROWID_OBJECT = c.NODE_ID
      JOIN PARTY_XREF xr ON xr.ORIG_ROWID_OBJECT = c.CLUSTER_ROOT
     WHERE c.WORK_STREAM = 'CROSS'
       AND c.CLUSTER_ROOT <> c.NODE_ID;
    COMMIT;

    MERGE INTO PARTY_XREF x
    USING (
        SELECT NODE_ID, CLUSTER_ROOT
          FROM STG_NODE_CLUSTER
         WHERE WORK_STREAM = 'CROSS'
    ) c
    ON (x.ORIG_ROWID_OBJECT = c.NODE_ID)
    WHEN MATCHED THEN
        UPDATE SET x.ROWID_OBJECT = c.CLUSTER_ROOT
         WHERE x.ROWID_OBJECT <> c.CLUSTER_ROOT;

    COMMIT;
END PRC_RUN_CROSS_TYPE_MERGE;
/

-----------------------

CREATE OR REPLACE PROCEDURE PRC_ORCHESTRATE_MDM_MERGE (
    p_run_mode IN VARCHAR2 DEFAULT 'DELTA' -- Pass 'DAY1' or 'DELTA'
) AS
    v_running_jobs NUMBER := 2;
    v_failed_cnt   NUMBER := 0;
BEGIN
    -- 1. Launch BSNS cluster job asynchronously
    DBMS_SCHEDULER.CREATE_JOB (
        job_name   => 'JOB_MDM_MERGE_BSNS',
        job_type   => 'PLSQL_BLOCK',
        job_action => 'BEGIN PRC_RUN_MDM_MERGE_BY_TYPE(''BSNS'', ''' || UPPER(p_run_mode) || '''); END;',
        enabled    => TRUE,
        auto_drop  => TRUE
    );

    -- 2. Launch INDVL cluster job asynchronously (Runs in parallel with BSNS)
    DBMS_SCHEDULER.CREATE_JOB (
        job_name   => 'JOB_MDM_MERGE_INDVL',
        job_type   => 'PLSQL_BLOCK',
        job_action => 'BEGIN PRC_RUN_MDM_MERGE_BY_TYPE(''INDVL'', ''' || UPPER(p_run_mode) || '''); END;',
        enabled    => TRUE,
        auto_drop  => TRUE
    );

    -- 3. Poll until both parallel jobs finish
    LOOP
        SELECT COUNT(*)
          INTO v_running_jobs
          FROM USER_SCHEDULER_JOBS
         WHERE JOB_NAME IN ('JOB_MDM_MERGE_BSNS', 'JOB_MDM_MERGE_INDVL');

        EXIT WHEN v_running_jobs = 0;
        DBMS_SESSION.SLEEP(2);
    END LOOP;

    -- 4. Verify both parallel jobs succeeded
    SELECT COUNT(*)
      INTO v_failed_cnt
      FROM USER_SCHEDULER_JOB_RUN_DETAILS
     WHERE JOB_NAME IN ('JOB_MDM_MERGE_BSNS', 'JOB_MDM_MERGE_INDVL')
       AND LOG_DATE > SYSDATE - (1/24)
       AND STATUS <> 'SUCCEEDED';

    IF v_failed_cnt > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'BSNS or INDVL merge job failed. Check USER_SCHEDULER_JOB_RUN_DETAILS.');
    END IF;

    -- 5. Execute Phase 3: Sequential Cross-Type Reconciliation
    PRC_RUN_CROSS_TYPE_MERGE(p_run_mode => UPPER(p_run_mode));
END PRC_ORCHESTRATE_MDM_MERGE;
/
--------------------- Test data

-------------------------------------------------------------------------------
-- SAMPLE DML: SEEDING & RUNNING DAY 1
-------------------------------------------------------------------------------
-- 1. Example PARTY_XREF rows (including a pre-existing CROSS merge 10 <- 20)
INSERT INTO PARTY_XREF VALUES (1,  1,  'INDVL');
INSERT INTO PARTY_XREF VALUES (2,  2,  'INDVL');
INSERT INTO PARTY_XREF VALUES (3,  3,  'INDVL');
INSERT INTO PARTY_XREF VALUES (10, 10, 'BSNS');
INSERT INTO PARTY_XREF VALUES (20, 10, 'INDVL'); -- Historical CROSS merge in PARTY_XREF
INSERT INTO PARTY_XREF VALUES (30, 30, 'BSNS');
INSERT INTO PARTY_XREF VALUES (40, 40, 'BSNS');

-- 2. Example Day 1 PME_MTCH rows
INSERT INTO PME_MTCH (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP) VALUES (1,  2,  'INDVL', 'INDVL');
INSERT INTO PME_MTCH (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP) VALUES (2,  3,  'INDVL', 'INDVL');
INSERT INTO PME_MTCH (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP) VALUES (30, 40, 'BSNS',  'BSNS'); -- Degree-1 fast-path pair
COMMIT;

-- 3. Run Day 1 Full Volume Merge
--    (To run synchronously in a single session without DBMS_SCHEDULER, call the 3 procs directly;
--     to run BSNS & INDVL in parallel background jobs, call PRC_ORCHESTRATE_MDM_MERGE('DAY1'))
BEGIN
    PRC_RUN_MDM_MERGE_BY_TYPE('BSNS',  'DAY1');
    PRC_RUN_MDM_MERGE_BY_TYPE('INDVL', 'DAY1');
    PRC_RUN_CROSS_TYPE_MERGE('DAY1');
END;
/

-------------------------------------------------------------------------------
-- SAMPLE DML: RUNNING DAY 2 (DELTA + AUTO-UNMERGE + MANUAL OVERRIDE)
-------------------------------------------------------------------------------
TRUNCATE TABLE PME_MTCH;
TRUNCATE TABLE PME_SENT_PARTIES;

-- Suppose INDVL node 3 had a core attribute change, was sent to PME, and returned 0 matches
INSERT INTO PME_SENT_PARTIES (PARTY_TYP, NODE_ID) VALUES ('INDVL', 3);

-- Suppose a steward manually unmerged the historical CROSS pair (10, 20)
-- (Notice we can pass (20, 10) out of order; the trigger normalizes it to (10, 20)!)
INSERT INTO PME_MANUAL_OVERRIDES (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, ISMERGE, ISUNMERGE)
VALUES (20, 10, 'INDVL', 'BSNS', 0, 1);
COMMIT;

-- Run Day 2 Delta Merge
BEGIN
    PRC_RUN_MDM_MERGE_BY_TYPE('BSNS',  'DELTA');
    PRC_RUN_MDM_MERGE_BY_TYPE('INDVL', 'DELTA');
    PRC_RUN_CROSS_TYPE_MERGE('DELTA');
END;
/

-- Inspect Final Results
SELECT * FROM PARTY_XREF ORDER BY ORIG_ROWID_OBJECT;
SELECT * FROM PME_CLUSTER_RESULT ORDER BY CLUSTER_ROOT, NODE_ID;

