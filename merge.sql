/*
1. Physical Staging Table DDL (Partitioned by WORK_STREAM)
Each staging table is list-partitioned into three isolated work streams:

P_BSNS (Used exclusively by the parallel BSNS job)

P_INDVL (Used exclusively by the parallel INDVL job)

P_CROSS (Used exclusively by the sequential CROSS job)

Because Oracle allows two sessions to run ALTER TABLE ... TRUNCATE PARTITION and INSERT INTO ... PARTITION (...) on different partitions of the same physical table simultaneously, BSNS and INDVL run in parallel with zero lock contention.
*/

-- 1. Physical Staging Table for Active Undirected Edges
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
CREATE INDEX IDX_STG_EDGES_UV ON STG_ACTIVE_EDGES(WORK_STREAM, U_NODE, V_NODE) LOCAL NOLOGGING;

-- 2. Physical Staging Table for Iterative Cluster Convergence
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

-- 3. Physical Staging Table for Impacted Nodes & Dependencies
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

-- 4. Physical Staging Table for Active Unmerge Rules
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

/*
2. Phase 1 & 2 Procedure (BSNS and INDVL Using Physical Tables)
Notice how every TRUNCATE targets only the active partition (P_BSNS or P_INDVL), and every query filters on WORK_STREAM = p_party_typ so Oracle performs Partition Pruning to a single partition.
*/

CREATE OR REPLACE PROCEDURE PRC_RUN_MDM_MERGE_BY_TYPE (
    p_party_typ IN VARCHAR2,                 -- Pass 'BSNS' or 'INDVL'
    p_run_mode  IN VARCHAR2 DEFAULT 'DELTA'  -- Pass 'DAY1' or 'DELTA'
) AS
    v_part_name VARCHAR2(30) := 'P_' || UPPER(p_party_typ);

    -- Internal helper: Truncate only this stream's partition on a staging table
    PROCEDURE truncate_stream_partition (p_table_name IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name ||
                          ' TRUNCATE PARTITION ' || v_part_name || ' UPDATE INDEXES';
    END truncate_stream_partition;

    -- Internal helper: Min-Label Propagation + Pointer Jumping on Physical Tables
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

    -- Clear ONLY this party_typ's partitions (Zero lock impact on the other stream)
    truncate_stream_partition('STG_ACTIVE_EDGES');
    truncate_stream_partition('STG_NODE_CLUSTER');
    truncate_stream_partition('STG_IMPACTED_NODES');
    truncate_stream_partition('STG_UNMERGE_RULES');

    ---------------------------------------------------------------------------
    -- STEP 1: ACCUMULATE INTRA-TYPE PME_MTCH_HIST
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
        USING (
            SELECT DISTINCT
                   x1.PARTY_TYP,
                   LEAST(x1.ROWID_OBJECT, x1.ORIG_ROWID_OBJECT)    AS ID1,
                   GREATEST(x1.ROWID_OBJECT, x1.ORIG_ROWID_OBJECT) AS ID2
              FROM PARTY_XREF x1
              JOIN PARTY_XREF x2 ON x1.ROWID_OBJECT = x2.ORIG_ROWID_OBJECT
             WHERE x1.ROWID_OBJECT <> x1.ORIG_ROWID_OBJECT
               AND x1.PARTY_TYP = p_party_typ
               AND x2.PARTY_TYP = p_party_typ
        ) x
        ON (h.PARTY_TYP = x.PARTY_TYP AND h.ID1 = x.ID1 AND h.ID2 = x.ID2)
        WHEN NOT MATCHED THEN
            INSERT (PARTY_TYP, ID1, ID2, CREATED_DT)
            VALUES (x.PARTY_TYP, x.ID1, x.ID2, SYSDATE);
    END IF;

    MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
    USING (
        SELECT DISTINCT
               p_party_typ            AS PARTY_TYP,
               LEAST(m.ID1, m.ID2)    AS ID1,
               GREATEST(m.ID1, m.ID2) AS ID2
          FROM PME_MTCH m
         WHERE m.PARTY_TYP = p_party_typ
           AND m.ID1 <> m.ID2
    ) m
    ON (h.PARTY_TYP = m.PARTY_TYP AND h.ID1 = m.ID1 AND h.ID2 = m.ID2)
    WHEN NOT MATCHED THEN
        INSERT (PARTY_TYP, ID1, ID2, CREATED_DT)
        VALUES (m.PARTY_TYP, m.ID1, m.ID2, SYSDATE);

    COMMIT;

    ---------------------------------------------------------------------------
    -- STEP 2: BUILD INTRA-TYPE SUBGRAPH IN PHYSICAL STAGING TABLES
    ---------------------------------------------------------------------------
    IF UPPER(p_run_mode) = 'DAY1' THEN
        INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT p_party_typ, LEAST(ID1, ID2), GREATEST(ID1, ID2)
          FROM PME_MANUAL_OVERRIDES
         WHERE ISUNMERGE = 1
           AND OVR_TYPE  = p_party_typ
           AND ID1 <> ID2;
        COMMIT;

        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
        WITH EFFECTIVE_PAIRS AS (
            SELECT ID1, ID2
              FROM PME_MTCH_HIST
             WHERE PARTY_TYP = p_party_typ
            UNION
            SELECT LEAST(ID1, ID2), GREATEST(ID1, ID2)
              FROM PME_MANUAL_OVERRIDES
             WHERE ISMERGE  = 1
               AND OVR_TYPE = p_party_typ
               AND ID1 <> ID2
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM STG_UNMERGE_RULES
             WHERE WORK_STREAM = p_party_typ
        )
        SELECT p_party_typ, ID1, ID2 FROM EFFECTIVE_PAIRS
        UNION ALL
        SELECT p_party_typ, ID2, ID1 FROM EFFECTIVE_PAIRS;
        COMMIT;

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
          );
        COMMIT;

    ELSE
        -- DELTA MODE FOR THIS PARTY_TYP
        INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
        SELECT p_party_typ, NODE_ID
          FROM (
            SELECT ID1 AS NODE_ID FROM PME_MTCH WHERE PARTY_TYP = p_party_typ AND ID1 <> ID2
            UNION
            SELECT ID2 FROM PME_MTCH WHERE PARTY_TYP = p_party_typ AND ID1 <> ID2
            UNION
            SELECT ID1 FROM PME_MANUAL_OVERRIDES WHERE OVR_TYPE = p_party_typ AND (ISMERGE = 1 OR ISUNMERGE = 1)
            UNION
            SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE OVR_TYPE = p_party_typ AND (ISMERGE = 1 OR ISUNMERGE = 1)
          );
        COMMIT;

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
                           ON p.ORIG_ROWID_OBJECT = i.NODE_ID
                          AND i.WORK_STREAM = p_party_typ
                        WHERE p.PARTY_TYP = p_party_typ
                       UNION
                       SELECT NODE_ID FROM STG_IMPACTED_NODES WHERE WORK_STREAM = p_party_typ
                   )
                UNION
                SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END AS DEP_NODE
                  FROM PME_MTCH_HIST h
                  JOIN STG_IMPACTED_NODES i
                    ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID)
                   AND i.WORK_STREAM = p_party_typ
                 WHERE h.PARTY_TYP = p_party_typ
            )
            SELECT DISTINCT p_party_typ, d.DEP_NODE
              FROM NEW_DEPS d
             WHERE NOT EXISTS (
                   SELECT 1 FROM STG_IMPACTED_NODES e
                    WHERE e.WORK_STREAM = p_party_typ
                      AND e.NODE_ID = d.DEP_NODE
             );

            EXIT WHEN SQL%ROWCOUNT = 0;
            COMMIT;
        END LOOP;
        COMMIT;

        INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
        SELECT DISTINCT p_party_typ, LEAST(o.ID1, o.ID2), GREATEST(o.ID1, o.ID2)
          FROM PME_MANUAL_OVERRIDES o
         WHERE o.ISUNMERGE = 1
           AND o.OVR_TYPE  = p_party_typ
           AND o.ID1 <> o.ID2
           AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID1)
             OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID2));
        COMMIT;

        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
        WITH EFFECTIVE_DELTA_PAIRS AS (
            SELECT h.ID1, h.ID2
              FROM PME_MTCH_HIST h
             WHERE h.PARTY_TYP = p_party_typ
               AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = h.ID1)
                 OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = h.ID2))
            UNION
            SELECT LEAST(o.ID1, o.ID2), GREATEST(o.ID1, o.ID2)
              FROM PME_MANUAL_OVERRIDES o
             WHERE o.ISMERGE  = 1
               AND o.OVR_TYPE = p_party_typ
               AND o.ID1 <> o.ID2
               AND (EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID1)
                 OR EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = p_party_typ AND i.NODE_ID = o.ID2))
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE
              FROM STG_UNMERGE_RULES
             WHERE WORK_STREAM = p_party_typ
        )
        SELECT p_party_typ, ID1, ID2 FROM EFFECTIVE_DELTA_PAIRS
        UNION ALL
        SELECT p_party_typ, ID2, ID1 FROM EFFECTIVE_DELTA_PAIRS;
        COMMIT;
    END IF;

    -- Gather partition stats so Oracle picks optimal hash joins for 160M rows
    DBMS_STATS.GATHER_TABLE_STATS(USER, 'STG_ACTIVE_EDGES',   PARTNAME => v_part_name, GRANULARITY => 'PARTITION');
    DBMS_STATS.GATHER_TABLE_STATS(USER, 'STG_IMPACTED_NODES', PARTNAME => v_part_name, GRANULARITY => 'PARTITION');

    ---------------------------------------------------------------------------
    -- STEP 3: TRANSITIVE BRIDGE PRUNING & CONVERGENCE
    ---------------------------------------------------------------------------
    DECLARE
        v_unmerge_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_unmerge_cnt
          FROM STG_UNMERGE_RULES
         WHERE WORK_STREAM = p_party_typ;

        IF v_unmerge_cnt > 0 THEN
            run_label_propagation(p_exclude_targets => TRUE);

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
                    WHERE m.ISMERGE  = 1
                      AND m.OVR_TYPE = p_party_typ
                      AND LEAST(m.ID1, m.ID2)    = LEAST(e.U_NODE, e.V_NODE)
                      AND GREATEST(m.ID1, m.ID2) = GREATEST(e.U_NODE, e.V_NODE)
               );
            COMMIT;
        END IF;

        run_label_propagation(p_exclude_targets => FALSE);
    END;

    ---------------------------------------------------------------------------
    -- STEP 4: STAGE RESULTS INTO THIS PARTY_TYP'S PARTITION
    ---------------------------------------------------------------------------
    DELETE FROM PME_CLUSTER_RESULT
     WHERE PARTY_TYP = p_party_typ
       AND NODE_ID IN (
           SELECT NODE_ID FROM STG_NODE_CLUSTER WHERE WORK_STREAM = p_party_typ
       );

    INSERT /*+ APPEND */ INTO PME_CLUSTER_RESULT (PARTY_TYP, CLUSTER_ROOT, NODE_ID, UPDATED_DT)
    SELECT p_party_typ, CLUSTER_ROOT, NODE_ID, SYSDATE
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


/*
3. Phase 3 Procedure (CROSS Reconciliation Using Physical Tables)
Phase 3 uses WORK_STREAM = 'CROSS' (PARTITION P_CROSS) on the physical staging tables:
*/

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
                   'CROSS'                                          AS PARTY_TYP,
                   LEAST(x1.ROWID_OBJECT, x1.ORIG_ROWID_OBJECT)    AS ID1,
                   GREATEST(x1.ROWID_OBJECT, x1.ORIG_ROWID_OBJECT) AS ID2
              FROM PARTY_XREF x1
              JOIN PARTY_XREF x2 ON x1.ROWID_OBJECT = x2.ORIG_ROWID_OBJECT
             WHERE x1.ROWID_OBJECT <> x1.ORIG_ROWID_OBJECT
               AND x1.PARTY_TYP <> x2.PARTY_TYP
        ) x
        ON (h.PARTY_TYP = x.PARTY_TYP AND h.ID1 = x.ID1 AND h.ID2 = x.ID2)
        WHEN NOT MATCHED THEN
            INSERT (PARTY_TYP, ID1, ID2, CREATED_DT)
            VALUES (x.PARTY_TYP, x.ID1, x.ID2, SYSDATE);
        COMMIT;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 2: SEED IMPACTED CROSS-TYPE NODES INTO PARTITION P_CROSS
    ---------------------------------------------------------------------------
    INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
    SELECT 'CROSS', NODE_ID
      FROM (
        SELECT ID1 AS NODE_ID FROM PME_MANUAL_OVERRIDES WHERE OVR_TYPE = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
        UNION
        SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE OVR_TYPE = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
        UNION
        SELECT ID1 FROM PME_MTCH_HIST WHERE PARTY_TYP = 'CROSS'
        UNION
        SELECT ID2 FROM PME_MTCH_HIST WHERE PARTY_TYP = 'CROSS'
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
    SELECT DISTINCT 'CROSS', LEAST(o.ID1, o.ID2), GREATEST(o.ID1, o.ID2)
      FROM PME_MANUAL_OVERRIDES o
     WHERE o.ISUNMERGE = 1
       AND o.ID1 <> o.ID2
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
        SELECT LEAST(o.ID1, o.ID2), GREATEST(o.ID1, o.ID2)
          FROM PME_MANUAL_OVERRIDES o
         WHERE o.ISMERGE = 1
           AND o.ID1 <> o.ID2
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
                      AND LEAST(m.ID1, m.ID2)    = LEAST(e.U_NODE, e.V_NODE)
                      AND GREATEST(m.ID1, m.ID2) = GREATEST(e.U_NODE, e.V_NODE)
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

    INSERT INTO PME_CLUSTER_RESULT (PARTY_TYP, CLUSTER_ROOT, NODE_ID, UPDATED_DT)
    SELECT x.PARTY_TYP, c.CLUSTER_ROOT, c.NODE_ID, SYSDATE
      FROM STG_NODE_CLUSTER c
      JOIN PARTY_XREF x ON x.ORIG_ROWID_OBJECT = c.NODE_ID
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
