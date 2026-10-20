CREATE OR REPLACE PACKAGE PKG_MDM_MERGE AS

    -- 1. Utility: Truncate a specific partition (P_BSNS, P_INDVL, P_CROSS)
    PROCEDURE prc_truncate_part (
        p_table_name  IN VARCHAR2,
        p_work_stream IN VARCHAR2
    );

    -- 2. Shared Core Engine: Min-Label Propagation + Pointer Jumping (Dynamic SQL)
    PROCEDURE prc_converge_labels (
        p_work_stream     IN VARCHAR2,
        p_exclude_targets IN BOOLEAN DEFAULT FALSE,
        p_init_nodes      IN BOOLEAN DEFAULT TRUE
    );

    -- 3. Shared 2-Pass Unmerge Bridge Cutter + Graph Resolver
    PROCEDURE prc_resolve_graph (
        p_work_stream IN VARCHAR2
    );

    -- 4. Shared Metadata-Driven Auto-Link Prevention (TAX_ID, BIRTH_DT, etc.)
    PROCEDURE prc_apply_link_prevention (
        p_work_stream IN VARCHAR2
    );

    -- 5. Pre-Processor: Load PME_MTCH_HIST & Apply Sent-Party Soft Deletes
    PROCEDURE prc_load_pme_mtch_hist (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    );

    -- 6. Intra-Type Merge Stream ('BSNS' or 'INDVL')
    PROCEDURE prc_run_merge_by_type (
        p_party_typ IN VARCHAR2,
        p_run_mode  IN VARCHAR2 DEFAULT 'DELTA'
    );

    -- 7. Cross-Type Stitching & Reconciliation ('CROSS')
    PROCEDURE prc_run_cross_type_merge (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    );

    -- 8. Standalone Party XREF Synchronization & Circuit Breaker Validation
    PROCEDURE prc_sync_party_xref (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    );

    -- 9. Master Orchestrator
    PROCEDURE prc_orchestrate_mdm_merge (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    );

END PKG_MDM_MERGE;
/

CREATE OR REPLACE PACKAGE BODY PKG_MDM_MERGE AS

    ---------------------------------------------------------------------------
    -- 1. UTILITY: PARTITION TRUNCATE
    ---------------------------------------------------------------------------
    PROCEDURE prc_truncate_part (
        p_table_name  IN VARCHAR2,
        p_work_stream IN VARCHAR2
    ) IS
        v_part_name VARCHAR2(30) := 'P_' || UPPER(TRIM(p_work_stream));
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name) ||
                          ' TRUNCATE PARTITION ' || DBMS_ASSERT.SIMPLE_SQL_NAME(v_part_name) ||
                          ' UPDATE INDEXES';
    END prc_truncate_part;

    ---------------------------------------------------------------------------
    -- 2. SHARED CORE ENGINE: MIN-LABEL PROPAGATION + POINTER JUMPING
    ---------------------------------------------------------------------------
    PROCEDURE prc_converge_labels (
        p_work_stream     IN VARCHAR2,
        p_exclude_targets IN BOOLEAN DEFAULT FALSE,
        p_init_nodes      IN BOOLEAN DEFAULT TRUE
    ) IS
        v_part         VARCHAR2(30) := 'P_' || UPPER(TRIM(p_work_stream));
        v_par_hint     VARCHAR2(30) := CASE WHEN UPPER(p_work_stream) = 'CROSS' THEN '' ELSE '/*+ PARALLEL(4) */' END;
        v_rows_updated NUMBER := 1;
        v_prop_sql     VARCHAR2(4000);
        v_jump_sql     VARCHAR2(4000);
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        IF p_init_nodes THEN
            prc_truncate_part('STG_NODE_CLUSTER', p_work_stream);

            EXECUTE IMMEDIATE
                'INSERT /*+ APPEND ' || v_par_hint || ' */ INTO STG_NODE_CLUSTER PARTITION (' || v_part || ') ' ||
                '(WORK_STREAM, NODE_ID, CLUSTER_ROOT) ' ||
                'SELECT :1, NODE_ID, NODE_ID FROM STG_IMPACTED_NODES PARTITION (' || v_part || ')'
                USING p_work_stream;
            COMMIT;
        END IF;

        v_prop_sql :=
            'MERGE ' || v_par_hint || ' INTO STG_NODE_CLUSTER PARTITION (' || v_part || ') c ' ||
            'USING ( ' ||
            '    SELECT e.U_NODE AS NODE_ID, ' ||
            '           MIN(nc.CLUSTER_ROOT) AS MIN_NEIGHBOR_ROOT ' ||
            '      FROM STG_ACTIVE_EDGES PARTITION (' || v_part || ') e ' ||
            '      JOIN STG_NODE_CLUSTER PARTITION (' || v_part || ') nc ' ||
            '        ON e.V_NODE = nc.NODE_ID ' ||
            CASE
                WHEN p_exclude_targets THEN
                    ' WHERE NOT EXISTS ( ' ||
                    '     SELECT 1 FROM STG_UNMERGE_RULES PARTITION (' || v_part || ') r ' ||
                    '      WHERE r.TARGET_NODE = e.U_NODE OR r.TARGET_NODE = e.V_NODE ' ||
                    ' ) '
                ELSE ''
            END ||
            '     GROUP BY e.U_NODE ' ||
            ') src ' ||
            'ON (c.NODE_ID = src.NODE_ID) ' ||
            'WHEN MATCHED THEN ' ||
            '    UPDATE SET c.CLUSTER_ROOT = src.MIN_NEIGHBOR_ROOT ' ||
            '     WHERE src.MIN_NEIGHBOR_ROOT < c.CLUSTER_ROOT';

        v_jump_sql :=
            'MERGE ' || v_par_hint || ' INTO STG_NODE_CLUSTER PARTITION (' || v_part || ') c ' ||
            'USING STG_NODE_CLUSTER PARTITION (' || v_part || ') p ' ||
            'ON (c.CLUSTER_ROOT = p.NODE_ID) ' ||
            'WHEN MATCHED THEN ' ||
            '    UPDATE SET c.CLUSTER_ROOT = p.CLUSTER_ROOT ' ||
            '     WHERE p.CLUSTER_ROOT < c.CLUSTER_ROOT';

        WHILE v_rows_updated > 0 LOOP
            EXECUTE IMMEDIATE v_prop_sql;
            v_rows_updated := SQL%ROWCOUNT;
            COMMIT;

            EXECUTE IMMEDIATE v_jump_sql;
            v_rows_updated := v_rows_updated + SQL%ROWCOUNT;
            COMMIT;
        END LOOP;
    END prc_converge_labels;

    ---------------------------------------------------------------------------
    -- 3. SHARED 2-PASS UNMERGE BRIDGE CUTTER + GRAPH RESOLVER
    ---------------------------------------------------------------------------
    PROCEDURE prc_resolve_graph (
        p_work_stream IN VARCHAR2
    ) IS
        v_part        VARCHAR2(30) := 'P_' || UPPER(TRIM(p_work_stream));
        v_unmerge_cnt NUMBER := 0;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM STG_UNMERGE_RULES PARTITION (' || v_part || ')'
            INTO v_unmerge_cnt;

        IF v_unmerge_cnt > 0 THEN
            prc_converge_labels(
                p_work_stream     => p_work_stream,
                p_exclude_targets => TRUE,
                p_init_nodes      => TRUE
            );

            EXECUTE IMMEDIATE
                'INSERT INTO PME_TRANSITIVE_CUT_LOG ( ' ||
                '    WORK_STREAM, UNMERGE_ANCHOR, UNMERGE_TARGET, CUT_NODE_1, CUT_NODE_2, LOGGED_DT ' ||
                ') ' ||
                'SELECT DISTINCT :1, r.ANCHOR_NODE, r.TARGET_NODE, ' ||
                '       LEAST(e.U_NODE, e.V_NODE), GREATEST(e.U_NODE, e.V_NODE), SYSDATE ' ||
                '  FROM STG_UNMERGE_RULES PARTITION (' || v_part || ') r ' ||
                '  JOIN STG_NODE_CLUSTER  PARTITION (' || v_part || ') ca ON ca.NODE_ID = r.ANCHOR_NODE ' ||
                '  JOIN STG_ACTIVE_EDGES  PARTITION (' || v_part || ') e  ON e.V_NODE   = r.TARGET_NODE ' ||
                '  JOIN STG_NODE_CLUSTER  PARTITION (' || v_part || ') cw ON cw.NODE_ID = e.U_NODE ' ||
                ' WHERE cw.CLUSTER_ROOT = ca.CLUSTER_ROOT ' ||
                '   AND NOT EXISTS ( ' ||
                '       SELECT 1 FROM PME_MANUAL_OVERRIDES m ' ||
                '        WHERE m.ISMERGE = 1 ' ||
                '          AND m.ID1     = LEAST(e.U_NODE, e.V_NODE) ' ||
                '          AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE) ' ||
                '   )'
                USING p_work_stream;

            EXECUTE IMMEDIATE
                'DELETE FROM STG_ACTIVE_EDGES PARTITION (' || v_part || ') e ' ||
                ' WHERE EXISTS ( ' ||
                '       SELECT 1 ' ||
                '         FROM STG_UNMERGE_RULES PARTITION (' || v_part || ') r ' ||
                '         JOIN STG_NODE_CLUSTER  PARTITION (' || v_part || ') ca ON ca.NODE_ID = r.ANCHOR_NODE ' ||
                '         JOIN STG_NODE_CLUSTER  PARTITION (' || v_part || ') cw ON cw.CLUSTER_ROOT = ca.CLUSTER_ROOT ' ||
                '        WHERE (e.U_NODE = cw.NODE_ID AND e.V_NODE = r.TARGET_NODE) ' ||
                '           OR (e.U_NODE = r.TARGET_NODE AND e.V_NODE = cw.NODE_ID) ' ||
                '   ) ' ||
                '   AND NOT EXISTS ( ' ||
                '       SELECT 1 FROM PME_MANUAL_OVERRIDES m ' ||
                '        WHERE m.ISMERGE = 1 ' ||
                '          AND m.ID1     = LEAST(e.U_NODE, e.V_NODE) ' ||
                '          AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE) ' ||
                '   )';
            COMMIT;
        END IF;

        prc_converge_labels(
            p_work_stream     => p_work_stream,
            p_exclude_targets => FALSE,
            p_init_nodes      => TRUE
        );
    END prc_resolve_graph;

    ---------------------------------------------------------------------------
    -- 4. SHARED METADATA-DRIVEN AUTO-LINK PREVENTION (TAX_ID, BIRTH_DT, ETC.)
    ---------------------------------------------------------------------------
    PROCEDURE prc_apply_link_prevention (
        p_work_stream IN VARCHAR2
    ) IS
        v_part         VARCHAR2(30) := 'P_' || UPPER(TRIM(p_work_stream));
        v_conflict_cnt NUMBER := 0;
        v_pass         NUMBER := 0;
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
        prc_truncate_part('STG_NODE_PREV_ATTRS', p_work_stream);
        prc_truncate_part('STG_CONFLICT_NODES',  p_work_stream);

        FOR r IN (
            SELECT RULE_CODE, SOURCE_TABLE, PK_COLUMN, ATTR_EXPR
              FROM PME_LINK_PREVENTION_RULES
             WHERE IS_ACTIVE = 1
               AND (PARTY_TYP = 'ALL' OR PARTY_TYP = p_work_stream OR p_work_stream = 'CROSS')
             ORDER BY PRIORITY_ORDER
        ) LOOP
            EXECUTE IMMEDIATE
                'INSERT /*+ APPEND PARALLEL(4) */ INTO STG_NODE_PREV_ATTRS PARTITION (' || v_part || ') ' ||
                '(WORK_STREAM, NODE_ID, RULE_CODE, ATTR_VAL) ' ||
                'SELECT :1, c.NODE_ID, :2, TO_CHAR(' || r.ATTR_EXPR || ') ' ||
                '  FROM STG_NODE_CLUSTER PARTITION (' || v_part || ') c ' ||
                '  JOIN ' || DBMS_ASSERT.SQL_OBJECT_NAME(r.SOURCE_TABLE) || ' src ' ||
                '    ON src.' || DBMS_ASSERT.SIMPLE_SQL_NAME(r.PK_COLUMN) || ' = c.NODE_ID ' ||
                ' WHERE c.CLUSTER_ROOT IN ( ' ||
                '       SELECT CLUSTER_ROOT FROM STG_NODE_CLUSTER PARTITION (' || v_part || ') ' ||
                '        WHERE CLUSTER_ROOT <> NODE_ID ' ||
                '   ) ' ||
                '   AND (' || r.ATTR_EXPR || ') IS NOT NULL'
                USING p_work_stream, r.RULE_CODE;
            COMMIT;
        END LOOP;

        LOOP
            v_pass := v_pass + 1;
            prc_truncate_part('STG_CONFLICT_NODES', p_work_stream);

            INSERT /*+ APPEND PARALLEL(4) */ INTO STG_CONFLICT_NODES (
                WORK_STREAM, CLUSTER_ROOT, NODE_ID, RULE_CODE, NODE_VAL, ANCHOR_VAL
            )
            WITH CLUSTER_ATTR_COUNTS AS (
                SELECT c.CLUSTER_ROOT,
                       a.RULE_CODE,
                       a.ATTR_VAL,
                       COUNT(*)       AS VAL_FREQ,
                       MIN(c.NODE_ID) AS MIN_NODE_FOR_VAL,
                       COUNT(DISTINCT a.ATTR_VAL) OVER (
                           PARTITION BY c.CLUSTER_ROOT, a.RULE_CODE
                       ) AS DISTINCT_VAL_CNT
                  FROM STG_NODE_CLUSTER c
                  JOIN STG_NODE_PREV_ATTRS a
                    ON a.WORK_STREAM = c.WORK_STREAM
                   AND a.NODE_ID     = c.NODE_ID
                 WHERE c.WORK_STREAM = p_work_stream
                 GROUP BY c.CLUSTER_ROOT, a.RULE_CODE, a.ATTR_VAL
            ),
            CLUSTER_ANCHORS AS (
                SELECT CLUSTER_ROOT,
                       RULE_CODE,
                       MIN(ATTR_VAL) KEEP (
                           DENSE_RANK FIRST ORDER BY VAL_FREQ DESC, MIN_NODE_FOR_VAL ASC
                       ) AS ANCHOR_VAL
                  FROM CLUSTER_ATTR_COUNTS
                 WHERE DISTINCT_VAL_CNT > 1
                 GROUP BY CLUSTER_ROOT, RULE_CODE
            )
            SELECT p_work_stream,
                   c.CLUSTER_ROOT,
                   c.NODE_ID,
                   an.RULE_CODE,
                   a.ATTR_VAL AS NODE_VAL,
                   an.ANCHOR_VAL
              FROM CLUSTER_ANCHORS an
              JOIN STG_NODE_CLUSTER c
                ON c.WORK_STREAM  = p_work_stream
               AND c.CLUSTER_ROOT = an.CLUSTER_ROOT
              JOIN STG_NODE_PREV_ATTRS a
                ON a.WORK_STREAM = p_work_stream
               AND a.NODE_ID     = c.NODE_ID
               AND a.RULE_CODE   = an.RULE_CODE
             WHERE a.ATTR_VAL <> an.ANCHOR_VAL;

            v_conflict_cnt := SQL%ROWCOUNT;
            COMMIT;

            EXIT WHEN v_conflict_cnt = 0 OR v_pass > 5;

            UPDATE PME_MTCH_HIST h
               SET h.IS_ACTIVE = 0,
                   h.UPDT_TS   = SYSTIMESTAMP
             WHERE h.IS_ACTIVE = 1
               AND EXISTS (
                   SELECT 1
                     FROM STG_RAW_PAIRS rp
                     JOIN STG_CONFLICT_NODES cn
                       ON cn.WORK_STREAM = p_work_stream
                      AND (cn.NODE_ID = rp.ID1 OR cn.NODE_ID = rp.ID2)
                    WHERE rp.WORK_STREAM = p_work_stream
                      AND rp.IS_DEGREE_1 = 1
                      AND h.ID1 = rp.ID1
                      AND h.ID2 = rp.ID2
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1 AND m.ID1 = h.ID1 AND m.ID2 = h.ID2
               );

            UPDATE STG_NODE_CLUSTER c
               SET c.CLUSTER_ROOT = c.NODE_ID
             WHERE c.WORK_STREAM = p_work_stream
               AND EXISTS (
                   SELECT 1 FROM STG_CONFLICT_NODES cn
                    WHERE cn.WORK_STREAM = p_work_stream AND cn.NODE_ID = c.NODE_ID
               )
               AND EXISTS (
                   SELECT 1 FROM STG_RAW_PAIRS rp
                    WHERE rp.WORK_STREAM = p_work_stream
                      AND rp.IS_DEGREE_1 = 1
                      AND (rp.ID1 = c.NODE_ID OR rp.ID2 = c.NODE_ID)
               );
            COMMIT;

            INSERT INTO PME_LINK_PREVENTION_LOG (
                WORK_STREAM, RULE_CODE, CLUSTER_ROOT, CUT_NODE_1, CUT_NODE_2,
                CONFLICT_ID, CONFLICT_VAL, ANCHOR_VAL, LOGGED_TS
            )
            SELECT DISTINCT
                   p_work_stream, cn.RULE_CODE, cn.CLUSTER_ROOT,
                   LEAST(e.U_NODE, e.V_NODE), GREATEST(e.U_NODE, e.V_NODE),
                   cn.NODE_ID, cn.NODE_VAL, cn.ANCHOR_VAL, SYSTIMESTAMP
              FROM STG_CONFLICT_NODES cn
              JOIN STG_ACTIVE_EDGES e
                ON e.WORK_STREAM = p_work_stream AND e.U_NODE = cn.NODE_ID
              LEFT JOIN STG_NODE_PREV_ATTRS nbr_a
                ON nbr_a.WORK_STREAM = p_work_stream
               AND nbr_a.NODE_ID     = e.V_NODE
               AND nbr_a.RULE_CODE   = cn.RULE_CODE
             WHERE cn.WORK_STREAM = p_work_stream
               AND (nbr_a.ATTR_VAL IS NULL OR nbr_a.ATTR_VAL <> cn.NODE_VAL)
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND m.ID1     = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE)
               );

            UPDATE PME_MTCH_HIST h
               SET h.IS_ACTIVE = 0,
                   h.UPDT_TS   = SYSTIMESTAMP
             WHERE h.IS_ACTIVE = 1
               AND EXISTS (
                   SELECT 1
                     FROM STG_CONFLICT_NODES cn
                     JOIN STG_ACTIVE_EDGES e
                       ON e.WORK_STREAM = p_work_stream AND e.U_NODE = cn.NODE_ID
                     LEFT JOIN STG_NODE_PREV_ATTRS nbr_a
                       ON nbr_a.WORK_STREAM = p_work_stream
                      AND nbr_a.NODE_ID     = e.V_NODE
                      AND nbr_a.RULE_CODE   = cn.RULE_CODE
                    WHERE cn.WORK_STREAM = p_work_stream
                      AND (nbr_a.ATTR_VAL IS NULL OR nbr_a.ATTR_VAL <> cn.NODE_VAL)
                      AND h.ID1 = LEAST(e.U_NODE, e.V_NODE)
                      AND h.ID2 = GREATEST(e.U_NODE, e.V_NODE)
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1 AND m.ID1 = h.ID1 AND m.ID2 = h.ID2
               );

            DELETE FROM STG_ACTIVE_EDGES e
             WHERE e.WORK_STREAM = p_work_stream
               AND EXISTS (
                   SELECT 1
                     FROM STG_CONFLICT_NODES cn
                     LEFT JOIN STG_NODE_PREV_ATTRS nbr_a
                       ON nbr_a.WORK_STREAM = p_work_stream
                      AND nbr_a.RULE_CODE   = cn.RULE_CODE
                      AND nbr_a.NODE_ID     = CASE WHEN e.U_NODE = cn.NODE_ID THEN e.V_NODE ELSE e.U_NODE END
                    WHERE cn.WORK_STREAM = p_work_stream
                      AND (e.U_NODE = cn.NODE_ID OR e.V_NODE = cn.NODE_ID)
                      AND (nbr_a.ATTR_VAL IS NULL OR nbr_a.ATTR_VAL <> cn.NODE_VAL)
               )
               AND NOT EXISTS (
                   SELECT 1 FROM PME_MANUAL_OVERRIDES m
                    WHERE m.ISMERGE = 1
                      AND m.ID1     = LEAST(e.U_NODE, e.V_NODE)
                      AND m.ID2     = GREATEST(e.U_NODE, e.V_NODE)
               );
            COMMIT;

            UPDATE STG_NODE_CLUSTER c
               SET c.CLUSTER_ROOT = c.NODE_ID
             WHERE c.WORK_STREAM = p_work_stream
               AND c.CLUSTER_ROOT IN (
                   SELECT DISTINCT CLUSTER_ROOT
                     FROM STG_CONFLICT_NODES
                    WHERE WORK_STREAM = p_work_stream
               );
            COMMIT;

            prc_converge_labels(
                p_work_stream     => p_work_stream,
                p_exclude_targets => FALSE,
                p_init_nodes      => FALSE
            );
        END LOOP;
    END prc_apply_link_prevention;

    ---------------------------------------------------------------------------
    -- 5. PRE-PROCESSOR: LOAD PME_MTCH_HIST & EVALUATE SENT PARTIES
    ---------------------------------------------------------------------------
    PROCEDURE prc_load_pme_mtch_hist (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    ) IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        IF UPPER(p_run_mode) = 'DAY1' THEN
            MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
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
                       CASE WHEN x1.PARTY_TYP = x2.PARTY_TYP
                            THEN x1.PARTY_TYP         ELSE 'CROSS'              END AS CALC_PAIR_TYP
                  FROM PARTY_XREF x1
                  JOIN PARTY_XREF x2 ON x1.ROWID_OBJECT = x2.ORIG_ROWID_OBJECT
                 WHERE x1.ROWID_OBJECT <> x1.ORIG_ROWID_OBJECT
            ) x
            ON (h.PAIR_TYP = x.CALC_PAIR_TYP AND h.ID1 = x.ID1 AND h.ID2 = x.ID2)
            WHEN NOT MATCHED THEN
                INSERT (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, IS_ACTIVE, CRT_TS, UPDT_TS)
                VALUES (x.ID1, x.ID2, x.ID1_PARTY_TYP, x.ID2_PARTY_TYP, 1, SYSTIMESTAMP, SYSTIMESTAMP);
            COMMIT;
        END IF;

        UPDATE /*+ PARALLEL(4) */ PME_MTCH_HIST h
           SET h.IS_ACTIVE = 0,
               h.UPDT_TS   = SYSTIMESTAMP
         WHERE h.IS_ACTIVE = 1
           AND (
               EXISTS (
                   SELECT 1 FROM PME_SENT_PARTIES s
                    WHERE s.PARTY_TYP = h.ID1_PARTY_TYP AND s.NODE_ID = h.ID1
               )
               OR
               EXISTS (
                   SELECT 1 FROM PME_SENT_PARTIES s
                    WHERE s.PARTY_TYP = h.ID2_PARTY_TYP AND s.NODE_ID = h.ID2
               )
           );
        COMMIT;

        MERGE /*+ PARALLEL(4) */ INTO PME_MTCH_HIST h
        USING (
            SELECT DISTINCT
                   CASE WHEN m.ID1 < m.ID2 THEN m.ID1           ELSE m.ID2           END AS ID1,
                   CASE WHEN m.ID1 < m.ID2 THEN m.ID2           ELSE m.ID1           END AS ID2,
                   CASE WHEN m.ID1 < m.ID2 THEN m.ID1_PARTY_TYP ELSE m.ID2_PARTY_TYP END AS ID1_PARTY_TYP,
                   CASE WHEN m.ID1 < m.ID2 THEN m.ID2_PARTY_TYP ELSE m.ID1_PARTY_TYP END AS ID2_PARTY_TYP,
                   m.PAIR_TYP
              FROM PME_MTCH m
             WHERE m.ID1 <> m.ID2
        ) src
        ON (h.PAIR_TYP = src.PAIR_TYP AND h.ID1 = src.ID1 AND h.ID2 = src.ID2)
        WHEN MATCHED THEN
            UPDATE SET h.IS_ACTIVE = 1,
                       h.UPDT_TS   = SYSTIMESTAMP
             WHERE h.IS_ACTIVE = 0
        WHEN NOT MATCHED THEN
            INSERT (ID1, ID2, ID1_PARTY_TYP, ID2_PARTY_TYP, IS_ACTIVE, CRT_TS, UPDT_TS)
            VALUES (src.ID1, src.ID2, src.ID1_PARTY_TYP, src.ID2_PARTY_TYP, 1, SYSTIMESTAMP, SYSTIMESTAMP);
        COMMIT;
    END prc_load_pme_mtch_hist;

    ---------------------------------------------------------------------------
    -- 6. INTRA-TYPE MERGE STREAM ('BSNS' OR 'INDVL')
    ---------------------------------------------------------------------------
    PROCEDURE prc_run_merge_by_type (
        p_party_typ IN VARCHAR2,
        p_run_mode  IN VARCHAR2 DEFAULT 'DELTA'
    ) IS
        v_part_name VARCHAR2(30) := 'P_' || UPPER(TRIM(p_party_typ));
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        prc_truncate_part('STG_RAW_PAIRS',      p_party_typ);
        prc_truncate_part('STG_ACTIVE_EDGES',   p_party_typ);
        prc_truncate_part('STG_NODE_CLUSTER',   p_party_typ);
        prc_truncate_part('STG_IMPACTED_NODES', p_party_typ);
        prc_truncate_part('STG_UNMERGE_RULES',  p_party_typ);

        IF UPPER(p_run_mode) IN ('DAY1', 'FULL') THEN
            INSERT INTO STG_UNMERGE_RULES (WORK_STREAM, ANCHOR_NODE, TARGET_NODE)
            SELECT DISTINCT p_party_typ, ID1, ID2
              FROM PME_MANUAL_OVERRIDES
             WHERE PAIR_TYP  = p_party_typ
               AND ISUNMERGE = 1;
            COMMIT;

            INSERT /*+ APPEND PARALLEL(4) */ INTO STG_RAW_PAIRS (WORK_STREAM, ID1, ID2, IS_DEGREE_1)
            WITH EFFECTIVE_PAIRS AS (
                SELECT ID1, ID2 FROM PME_MTCH_HIST WHERE PAIR_TYP = p_party_typ AND IS_ACTIVE = 1
                UNION
                SELECT ID1, ID2 FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = p_party_typ AND ISMERGE = 1
                MINUS
                SELECT ANCHOR_NODE, TARGET_NODE FROM STG_UNMERGE_RULES WHERE WORK_STREAM = p_party_typ
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
            SELECT p_party_typ, ep.ID1, ep.ID2,
                   CASE
                       WHEN d1.DEG = 1 AND d2.DEG = 1
                        AND NOT EXISTS (
                            SELECT 1 FROM STG_UNMERGE_RULES r
                             WHERE r.WORK_STREAM = p_party_typ
                               AND (r.ANCHOR_NODE IN (ep.ID1, ep.ID2) OR r.TARGET_NODE IN (ep.ID1, ep.ID2))
                        )
                       THEN 1 ELSE 0
                   END
              FROM EFFECTIVE_PAIRS ep
              JOIN NODE_DEGREES d1 ON d1.NODE_ID = ep.ID1
              JOIN NODE_DEGREES d2 ON d2.NODE_ID = ep.ID2;
            COMMIT;

            INSERT /*+ APPEND PARALLEL(4) */ INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
            SELECT p_party_typ, ID1, ID2 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0
            UNION ALL
            SELECT p_party_typ, ID2, ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0;
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
                SELECT ORIG_ROWID_OBJECT FROM PARTY_XREF WHERE PARTY_TYP = p_party_typ AND ROWID_OBJECT <> ORIG_ROWID_OBJECT
                UNION
                SELECT ROWID_OBJECT      FROM PARTY_XREF WHERE PARTY_TYP = p_party_typ AND ROWID_OBJECT <> ORIG_ROWID_OBJECT
                UNION
                SELECT ID1 FROM PME_MTCH_HIST WHERE ID1_PARTY_TYP = p_party_typ AND IS_ACTIVE = 0
                UNION
                SELECT ID2 FROM PME_MTCH_HIST WHERE ID2_PARTY_TYP = p_party_typ AND IS_ACTIVE = 0
                MINUS
                SELECT ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
                MINUS
                SELECT ID2 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
              );
            COMMIT;

        ELSE
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
                            WHERE h.PAIR_TYP = p_party_typ AND (h.ID1 = s.NODE_ID OR h.ID2 = s.NODE_ID)
                       )
                   )
                UNION
                SELECT ID1 FROM PME_MTCH_HIST WHERE ID1_PARTY_TYP = p_party_typ AND IS_ACTIVE = 0 AND UPDT_TS >= SYSTIMESTAMP - INTERVAL '1' DAY
                UNION
                SELECT ID2 FROM PME_MTCH_HIST WHERE ID2_PARTY_TYP = p_party_typ AND IS_ACTIVE = 0 AND UPDT_TS >= SYSTIMESTAMP - INTERVAL '1' DAY
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
                             JOIN STG_IMPACTED_NODES i ON p.ORIG_ROWID_OBJECT = i.NODE_ID AND i.WORK_STREAM = p_party_typ
                            WHERE p.PARTY_TYP = p_party_typ
                           UNION
                           SELECT NODE_ID FROM STG_IMPACTED_NODES WHERE WORK_STREAM = p_party_typ
                       )
                    UNION
                    SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END
                      FROM PME_MTCH_HIST h
                      JOIN STG_IMPACTED_NODES i ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID) AND i.WORK_STREAM = p_party_typ
                     WHERE h.PAIR_TYP = p_party_typ
                       AND (h.IS_ACTIVE = 1 OR h.UPDT_TS >= SYSTIMESTAMP - INTERVAL '1' DAY)
                )
                SELECT DISTINCT p_party_typ, d.DEP_NODE
                  FROM NEW_DEPS d
                 WHERE NOT EXISTS (
                       SELECT 1 FROM STG_IMPACTED_NODES e WHERE e.WORK_STREAM = p_party_typ AND e.NODE_ID = d.DEP_NODE
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
                 WHERE h.PAIR_TYP  = p_party_typ
                   AND h.IS_ACTIVE = 1
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
                SELECT ANCHOR_NODE, TARGET_NODE FROM STG_UNMERGE_RULES WHERE WORK_STREAM = p_party_typ
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
            SELECT p_party_typ, ep.ID1, ep.ID2,
                   CASE
                       WHEN d1.DEG = 1 AND d2.DEG = 1
                        AND NOT EXISTS (
                            SELECT 1 FROM STG_UNMERGE_RULES r
                             WHERE r.WORK_STREAM = p_party_typ
                               AND (r.ANCHOR_NODE IN (ep.ID1, ep.ID2) OR r.TARGET_NODE IN (ep.ID1, ep.ID2))
                        )
                       THEN 1 ELSE 0
                   END
              FROM EFFECTIVE_DELTA_PAIRS ep
              JOIN NODE_DEGREES d1 ON d1.NODE_ID = ep.ID1
              JOIN NODE_DEGREES d2 ON d2.NODE_ID = ep.ID2;
            COMMIT;

            INSERT /*+ APPEND PARALLEL(4) */ INTO STG_ACTIVE_EDGES (WORK_STREAM, U_NODE, V_NODE)
            SELECT p_party_typ, ID1, ID2 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0
            UNION ALL
            SELECT p_party_typ, ID2, ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 0;
            COMMIT;

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

        -- Call Shared Graph Resolver (Unmerge bridge cuts + Convergence)
        prc_resolve_graph(p_work_stream => p_party_typ);

        -- Append pre-solved Degree-1 pairs
        INSERT /*+ APPEND PARALLEL(4) */ INTO STG_NODE_CLUSTER (WORK_STREAM, NODE_ID, CLUSTER_ROOT)
        SELECT p_party_typ, ID1, ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1
        UNION ALL
        SELECT p_party_typ, ID2, ID1 FROM STG_RAW_PAIRS WHERE WORK_STREAM = p_party_typ AND IS_DEGREE_1 = 1;
        COMMIT;

        -- Call Shared Auto-Link Prevention Rules (TAX_ID, BIRTH_DT, etc.)
        prc_apply_link_prevention(p_work_stream => p_party_typ);

        -- Update PME_CLUSTER_RESULT (PARTY_XREF update is decoupled and executed at the very end)
        IF UPPER(p_run_mode) IN ('DAY1', 'FULL') THEN
            prc_truncate_part('PME_CLUSTER_RESULT', p_party_typ);
        ELSE
            DELETE /*+ PARALLEL(4) */ FROM PME_CLUSTER_RESULT
             WHERE NODE_PARTY_TYP = p_party_typ
               AND NODE_ID IN (SELECT NODE_ID FROM STG_NODE_CLUSTER WHERE WORK_STREAM = p_party_typ);
        END IF;

        INSERT /*+ APPEND PARALLEL(4) */ INTO PME_CLUSTER_RESULT (
            CLUSTER_ROOT, NODE_ID, ROOT_PARTY_TYP, NODE_PARTY_TYP, UPDATED_DT
        )
        SELECT CLUSTER_ROOT, NODE_ID, p_party_typ, p_party_typ, SYSDATE
          FROM STG_NODE_CLUSTER
         WHERE WORK_STREAM = p_party_typ
           AND CLUSTER_ROOT <> NODE_ID;
        COMMIT;
    END prc_run_merge_by_type;

    ---------------------------------------------------------------------------
    -- 7. CROSS-TYPE STITCHING & RECONCILIATION ('CROSS')
    ---------------------------------------------------------------------------
    PROCEDURE prc_run_cross_type_merge (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    ) IS
        v_cross_cnt NUMBER := 0;
    BEGIN
        prc_truncate_part('STG_RAW_PAIRS',      'CROSS');
        prc_truncate_part('STG_ACTIVE_EDGES',   'CROSS');
        prc_truncate_part('STG_NODE_CLUSTER',   'CROSS');
        prc_truncate_part('STG_IMPACTED_NODES', 'CROSS');
        prc_truncate_part('STG_UNMERGE_RULES',  'CROSS');

        INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
        SELECT 'CROSS', NODE_ID
          FROM (
            SELECT ID1 AS NODE_ID FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
            UNION
            SELECT ID2 FROM PME_MANUAL_OVERRIDES WHERE PAIR_TYP = 'CROSS' AND (ISMERGE = 1 OR ISUNMERGE = 1)
            UNION
            SELECT ID1 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS' AND IS_ACTIVE = 1
            UNION
            SELECT ID2 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS' AND IS_ACTIVE = 1
            UNION
            SELECT ID1 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS' AND IS_ACTIVE = 0
            UNION
            SELECT ID2 FROM PME_MTCH_HIST WHERE PAIR_TYP = 'CROSS' AND IS_ACTIVE = 0
          );

        v_cross_cnt := SQL%ROWCOUNT;
        COMMIT;

        IF v_cross_cnt = 0 THEN
            RETURN;
        END IF;

        LOOP
            INSERT INTO STG_IMPACTED_NODES (WORK_STREAM, NODE_ID)
            WITH NEW_DEPS AS (
                SELECT r.NODE_ID AS DEP_NODE
                  FROM PME_CLUSTER_RESULT r
                 WHERE r.CLUSTER_ROOT IN (
                       SELECT r2.CLUSTER_ROOT
                         FROM PME_CLUSTER_RESULT r2
                         JOIN STG_IMPACTED_NODES i ON r2.NODE_ID = i.NODE_ID AND i.WORK_STREAM = 'CROSS'
                       UNION
                       SELECT NODE_ID FROM STG_IMPACTED_NODES WHERE WORK_STREAM = 'CROSS'
                 )
                UNION
                SELECT r.CLUSTER_ROOT AS DEP_NODE
                  FROM PME_CLUSTER_RESULT r
                  JOIN STG_IMPACTED_NODES i ON r.NODE_ID = i.NODE_ID AND i.WORK_STREAM = 'CROSS'
                UNION
                SELECT x.ORIG_ROWID_OBJECT AS DEP_NODE
                  FROM PARTY_XREF x
                 WHERE x.ROWID_OBJECT IN (
                       SELECT p.ROWID_OBJECT
                         FROM PARTY_XREF p
                         JOIN STG_IMPACTED_NODES i ON p.ORIG_ROWID_OBJECT = i.NODE_ID AND i.WORK_STREAM = 'CROSS'
                 )
                UNION
                SELECT CASE WHEN h.ID1 = i.NODE_ID THEN h.ID2 ELSE h.ID1 END
                  FROM PME_MTCH_HIST h
                  JOIN STG_IMPACTED_NODES i ON (h.ID1 = i.NODE_ID OR h.ID2 = i.NODE_ID) AND i.WORK_STREAM = 'CROSS'
                 WHERE h.PAIR_TYP = 'CROSS' AND h.IS_ACTIVE = 1
                UNION
                SELECT CASE WHEN o.ID1 = i.NODE_ID THEN o.ID2 ELSE o.ID1 END
                  FROM PME_MANUAL_OVERRIDES o
                  JOIN STG_IMPACTED_NODES i ON (o.ID1 = i.NODE_ID OR o.ID2 = i.NODE_ID) AND i.WORK_STREAM = 'CROSS'
                 WHERE o.PAIR_TYP = 'CROSS' AND o.ISMERGE = 1
            )
            SELECT DISTINCT 'CROSS', d.DEP_NODE
              FROM NEW_DEPS d
             WHERE NOT EXISTS (
                   SELECT 1 FROM STG_IMPACTED_NODES e WHERE e.WORK_STREAM = 'CROSS' AND e.NODE_ID = d.DEP_NODE
             );

            EXIT WHEN SQL%ROWCOUNT = 0;
            COMMIT;
        END LOOP;

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
             WHERE h.IS_ACTIVE = 1
               AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = h.ID1)
               AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = h.ID2)
            UNION
            SELECT o.ID1, o.ID2
              FROM PME_MANUAL_OVERRIDES o
             WHERE o.ISMERGE = 1
               AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID1)
               AND EXISTS (SELECT 1 FROM STG_IMPACTED_NODES i WHERE i.WORK_STREAM = 'CROSS' AND i.NODE_ID = o.ID2)
            MINUS
            SELECT ANCHOR_NODE, TARGET_NODE FROM STG_UNMERGE_RULES WHERE WORK_STREAM = 'CROSS'
        )
        SELECT 'CROSS', ID1, ID2 FROM EFFECTIVE_CROSS_PAIRS
        UNION ALL
        SELECT 'CROSS', ID2, ID1 FROM EFFECTIVE_CROSS_PAIRS;
        COMMIT;

        prc_resolve_graph(p_work_stream => 'CROSS');
        prc_apply_link_prevention(p_work_stream => 'CROSS');

        DELETE FROM PME_CLUSTER_RESULT
         WHERE NODE_ID IN (SELECT NODE_ID FROM STG_NODE_CLUSTER WHERE WORK_STREAM = 'CROSS');

        INSERT INTO PME_CLUSTER_RESULT (
            CLUSTER_ROOT, NODE_ID, ROOT_PARTY_TYP, NODE_PARTY_TYP, UPDATED_DT
        )
        SELECT c.CLUSTER_ROOT, c.NODE_ID, xr.PARTY_TYP, xn.PARTY_TYP, SYSDATE
          FROM STG_NODE_CLUSTER c
          JOIN PARTY_XREF xn ON xn.ORIG_ROWID_OBJECT = c.NODE_ID
          JOIN PARTY_XREF xr ON xr.ORIG_ROWID_OBJECT = c.CLUSTER_ROOT
         WHERE c.WORK_STREAM = 'CROSS'
           AND c.CLUSTER_ROOT <> c.NODE_ID;
        COMMIT;
    END prc_run_cross_type_merge;

    ---------------------------------------------------------------------------
    -- 8. STANDALONE PARTY XREF SYNCHRONIZATION & CIRCUIT BREAKER VALIDATION
    ---------------------------------------------------------------------------
    PROCEDURE prc_sync_party_xref (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    ) IS
        v_max_cluster_size NUMBER := 0;
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        -- Safety Quality Gate: Inspect max cluster size before publishing to PARTY_XREF
        SELECT NVL(MAX(cnt), 0)
          INTO v_max_cluster_size
          FROM (
            SELECT COUNT(*) + 1 AS cnt
              FROM PME_CLUSTER_RESULT
             GROUP BY CLUSTER_ROOT
          );

        IF v_max_cluster_size > 50000 THEN
            RAISE_APPLICATION_ERROR(
                -20002,
                'Circuit Breaker Triggered: Cluster size (' || v_max_cluster_size ||
                ') exceeds safety limit of 50,000. Inspect PME_CLUSTER_RESULT before syncing PARTY_XREF.'
            );
        END IF;

        IF UPPER(p_run_mode) = 'DELTA' THEN
            MERGE /*+ PARALLEL(8) */ INTO PARTY_XREF x
            USING (
                SELECT NODE_ID,
                       MIN(CLUSTER_ROOT) KEEP (
                           DENSE_RANK FIRST ORDER BY CASE WHEN WORK_STREAM = 'CROSS' THEN 1 ELSE 2 END
                       ) AS FINAL_CLUSTER_ROOT
                  FROM STG_NODE_CLUSTER
                 GROUP BY NODE_ID
            ) c
            ON (x.ORIG_ROWID_OBJECT = c.NODE_ID)
            WHEN MATCHED THEN
                UPDATE SET x.ROWID_OBJECT = c.FINAL_CLUSTER_ROOT
                 WHERE x.ROWID_OBJECT <> c.FINAL_CLUSTER_ROOT;
        ELSE
            MERGE /*+ PARALLEL(8) */ INTO PARTY_XREF x
            USING (
                SELECT x_in.ORIG_ROWID_OBJECT AS NODE_ID,
                       NVL(r.CLUSTER_ROOT, x_in.ORIG_ROWID_OBJECT) AS FINAL_CLUSTER_ROOT
                  FROM PARTY_XREF x_in
                  LEFT JOIN PME_CLUSTER_RESULT r
                    ON r.NODE_PARTY_TYP = x_in.PARTY_TYP
                   AND r.NODE_ID        = x_in.ORIG_ROWID_OBJECT
                 WHERE x_in.ROWID_OBJECT <> NVL(r.CLUSTER_ROOT, x_in.ORIG_ROWID_OBJECT)
            ) c
            ON (x.ORIG_ROWID_OBJECT = c.NODE_ID)
            WHEN MATCHED THEN
                UPDATE SET x.ROWID_OBJECT = c.FINAL_CLUSTER_ROOT;
        END IF;

        COMMIT;
    END prc_sync_party_xref;

    ---------------------------------------------------------------------------
    -- 9. MASTER ORCHESTRATOR
    ---------------------------------------------------------------------------
    PROCEDURE prc_orchestrate_mdm_merge (
        p_run_mode IN VARCHAR2 DEFAULT 'DELTA'
    ) IS
        v_running_jobs NUMBER := 2;
        v_failed_cnt   NUMBER := 0;
    BEGIN
        -- Stage 1: Load History & Evaluate Sent Parties
        prc_load_pme_mtch_hist(p_run_mode => UPPER(p_run_mode));

        -- Stage 2: Launch Parallel BSNS and INDVL Clustering Jobs
        DBMS_SCHEDULER.CREATE_JOB (
            job_name   => 'JOB_MDM_MERGE_BSNS',
            job_type   => 'PLSQL_BLOCK',
            job_action => 'BEGIN PKG_MDM_MERGE.prc_run_merge_by_type(''BSNS'', ''' || UPPER(p_run_mode) || '''); END;',
            enabled    => TRUE,
            auto_drop  => TRUE
        );

        DBMS_SCHEDULER.CREATE_JOB (
            job_name   => 'JOB_MDM_MERGE_INDVL',
            job_type   => 'PLSQL_BLOCK',
            job_action => 'BEGIN PKG_MDM_MERGE.prc_run_merge_by_type(''INDVL'', ''' || UPPER(p_run_mode) || '''); END;',
            enabled    => TRUE,
            auto_drop  => TRUE
        );

        LOOP
            SELECT COUNT(*)
              INTO v_running_jobs
              FROM USER_SCHEDULER_JOBS
             WHERE JOB_NAME IN ('JOB_MDM_MERGE_BSNS', 'JOB_MDM_MERGE_INDVL');

            EXIT WHEN v_running_jobs = 0;
            DBMS_SESSION.SLEEP(2);
        END LOOP;

        SELECT COUNT(*)
          INTO v_failed_cnt
          FROM USER_SCHEDULER_JOB_RUN_DETAILS
         WHERE JOB_NAME IN ('JOB_MDM_MERGE_BSNS', 'JOB_MDM_MERGE_INDVL')
           AND LOG_DATE > SYSDATE - (1/24)
           AND STATUS <> 'SUCCEEDED';

        IF v_failed_cnt > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'BSNS or INDVL merge job failed. Check USER_SCHEDULER_JOB_RUN_DETAILS.');
        END IF;

        -- Stage 3: Sequential Cross-Type Stitching & Reconciliation
        prc_run_cross_type_merge(p_run_mode => UPPER(p_run_mode));

        -- Stage 4: Sync PARTY_XREF (Executed once globally at the very end)
        prc_sync_party_xref(p_run_mode => UPPER(p_run_mode));
    END prc_orchestrate_mdm_merge;

END PKG_MDM_MERGE;
/
