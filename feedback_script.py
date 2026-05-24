import sqlite3
import pandas as pd
import os

DB_FILE = "data/mock_logs.db"
OUT_DIR = "feedback"

def export_feedback_split():
    if not os.path.exists(DB_FILE):
        print("[ERROR] mock_logs.db not found.")
        return
        
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)

    conn = sqlite3.connect(DB_FILE)

    # 1. Feedback for Posterior Update (LinTS)
    query_posterior = '''
        SELECT 
            d.impression_id AS Impression_ID,
            d.user_id AS TMNID,
            d.campaign_id AS Campaign_ID,
            json_extract(d.chosen_action_json, '$.domain') AS Domain,
            json_extract(d.chosen_action_json, '$.arm_campaign') AS arm_campaign,
            COALESCE(
                MAX(CASE WHEN i.event_type = 'OPEN'       THEN 'OPEN'       END),
                MAX(CASE WHEN i.event_type = 'DISMISS'    THEN 'DISMISS'    END),
                MAX(CASE WHEN i.event_type = 'IMPRESSION' THEN 'IMPRESSION' END),
                'NO_ACTION'
            ) AS Last_Action,
            COALESCE(MAX(i.reward_value), 0.0) AS Reward_Score,
            d.served_timestamp AS Sent_At
        FROM decision_log d
        LEFT JOIN interaction_log i ON d.impression_id = i.impression_id
        GROUP BY d.impression_id
        ORDER BY d.served_timestamp DESC
    '''

    # 2. Feedback for MLP Batch Retraining
    query_mlp = '''
        SELECT 
            d.impression_id AS Impression_ID,
            d.user_id AS TMNID,
            d.context_json AS Context_JSON,
            d.campaign_id AS Campaign_ID,
            json_extract(d.chosen_action_json, '$.domain') AS Domain,
            json_extract(d.chosen_action_json, '$.arm_campaign') AS arm_campaign,
            COALESCE(
                MAX(CASE WHEN i.event_type = 'OPEN'       THEN 'OPEN'       END),
                MAX(CASE WHEN i.event_type = 'DISMISS'    THEN 'DISMISS'    END),
                MAX(CASE WHEN i.event_type = 'IMPRESSION' THEN 'IMPRESSION' END),
                'NO_ACTION'
            ) AS Last_Action,
            COALESCE(MAX(i.reward_value), 0.0) AS Reward_Score,
            d.served_timestamp AS Sent_At
        FROM decision_log d
        LEFT JOIN interaction_log i ON d.impression_id = i.impression_id
        GROUP BY d.impression_id
        ORDER BY d.served_timestamp DESC
    '''

    # 3. Feedback for LLM Adaptive State
    query_llm = '''
        SELECT 
            d.user_id AS TMNID,
            d.campaign_id AS Campaign_ID,
            json_extract(d.chosen_action_json, '$.domain') AS Domain,
            json_extract(d.chosen_action_json, '$.arm_campaign') AS arm_campaign,
            json_extract(d.ui_payload, '$.title') AS Sent_Title,
            json_extract(d.ui_payload, '$.key_message') AS Sent_Msg,
            COALESCE(
                MAX(CASE WHEN i.event_type = 'OPEN'       THEN 'OPEN'       END),
                MAX(CASE WHEN i.event_type = 'DISMISS'    THEN 'DISMISS'    END),
                MAX(CASE WHEN i.event_type = 'IMPRESSION' THEN 'IMPRESSION' END),
                'NO_ACTION'
            ) AS Last_Action,
            COALESCE(MAX(i.reward_value), 0.0) AS Reward_Score,
            d.served_timestamp AS Sent_At
        FROM decision_log d
        LEFT JOIN interaction_log i ON d.impression_id = i.impression_id
        GROUP BY d.impression_id
        ORDER BY d.served_timestamp DESC
    '''

    # Execute and Export
    df_post = pd.read_sql_query(query_posterior, conn)
    df_post.to_csv(f"{OUT_DIR}/feedback_posterior.csv", index=False, encoding='utf-8-sig')

    df_mlp = pd.read_sql_query(query_mlp, conn)
    df_mlp.to_csv(f"{OUT_DIR}/feedback_mlp.csv", index=False, encoding='utf-8-sig')

    df_llm = pd.read_sql_query(query_llm, conn)
    df_llm.to_csv(f"{OUT_DIR}/feedback_llm_state.csv", index=False, encoding='utf-8-sig')

    conn.close()
    
    print(f"[SUCCESS] Exported Feedback Posterior : {len(df_post)} rows")
    print(f"[SUCCESS] Exported Feedback MLP       : {len(df_mlp)} rows")
    print(f"[SUCCESS] Exported Feedback LLM State : {len(df_llm)} rows")

    print("\n[SUMMARY] Posterior Data Breakdown:")
    print(df_post['Last_Action'].value_counts().to_string())
    print("-" * 40)

if __name__ == "__main__":
    export_feedback_split()