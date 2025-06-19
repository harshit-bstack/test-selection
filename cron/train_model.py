import os
import sys
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix
import datetime
import joblib
import argparse
import json
import psycopg2
from psycopg2.extras import execute_values

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db.database import get_db_connection


def load_data(file_path):
    """Loads data from a CSV file and performs initial preparation."""
    df = pd.read_csv(file_path)
    df['date_of_pr'] = pd.to_datetime(df['date_of_pr'])
    df = df.sort_values('date_of_pr')
    return df


def build_pipeline():
    """Builds a scikit-learn pipeline for preprocessing and modeling."""
    # Define categorical and numerical features
    categorical_features = ['file_name','file_extension', 'test_case_file_path', 'test_case_name']
    numerical_features = [
        'num_files_changed', 'failures_7_days', 'failures_14_days', 'failures_28_days',
        'changes_3_days', 'changes_14_days',
        'minimal_distance', 'common_tokens'
    ]

    # Create a preprocessor with ColumnTransformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', 'passthrough', numerical_features),
            ('cat', OrdinalEncoder(handle_unknown='use_encoded_value',
             unknown_value=-1), categorical_features)
        ],
        remainder='drop'
    )

    # Create the full pipeline
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', XGBClassifier(
            scale_pos_weight=9,
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100,
            verbosity=1,  # Quieter output
            use_label_encoder=False,
            eval_metric='logloss'  # Explicitly set eval_metric
        ))
    ])

    return model_pipeline, numerical_features + categorical_features


def train_and_evaluate(repo_name, file_path, model_output_path, metrics_output_path):
    """Loads data, trains the model, evaluates it, and saves the artifacts."""
    print("Loading and preparing data...")
    df = load_data(file_path)
    df['date_of_pr'] = pd.to_datetime(df['date_of_pr'])

    # Prepare features (X) and target (y)
    pipeline, feature_cols = build_pipeline()

    # Encode target variable
    target_col = 'actual_result'

    # Get unique pull requests and sort them by date to split data
    pr_dates = df[['pr_link', 'date_of_pr']].drop_duplicates().sort_values('date_of_pr')
    
    # Determine the split point for pull requests (last 20% for testing)
    split_index = int(len(pr_dates) * 0.8)
    test_pr_links = pr_dates['pr_link'].iloc[split_index:]

    # Split the dataframe based on the test pull requests
    train_df = df[~df['pr_link'].isin(test_pr_links)]
    test_df = df[df['pr_link'].isin(test_pr_links)]

    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    X_test = test_df[feature_cols]
    y_test = test_df[target_col]

    print(f"Training data size: {len(X_train)} samples")
    print(f"Testing data size: {len(X_test)} samples")
    print(f"Number of PRs in training set: {train_df['pr_link'].nunique()}")
    print(f"Number of PRs in testing set: {test_df['pr_link'].nunique()}")

    print("\nTraining model...")
    pipeline.fit(X_train, y_train)

    print("\nSaving model pipeline...")
    os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
    joblib.dump(pipeline, model_output_path)
 
    print(f"Model saved to {model_output_path}")

    print("\nEvaluating model...")
    y_pred = pipeline.predict(X_test)

    # Generate and print evaluation metrics
    report = classification_report(y_test, y_pred, output_dict=True)
    matrix = confusion_matrix(y_test, y_pred)

    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))

    print("\nConfusion Matrix:")
    print(matrix)

    print(f"\nSaving metrics to {metrics_output_path}...")
    metrics = {
        'classification_report': report,
        'confusion_matrix': matrix.tolist()
    }
    os.makedirs(os.path.dirname(metrics_output_path), exist_ok=True)
    with open(metrics_output_path, 'w') as f:
        json.dump(metrics, f, indent=4)
    print("Metrics saved.")

    print("\nSaving results to database...")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. Insert into checkpoint_details
        date_of_checkpoint = datetime.datetime.now()
        cur.execute(
            """
            INSERT INTO checkpoint_details (repo_name, date_of_checkpoint, model_path, data_path, meta_data)
            VALUES (%s, %s, %s, %s, %s) RETURNING checkpoint
            """,
            (repo_name, date_of_checkpoint, model_output_path, file_path, json.dumps(metrics))
        )
        checkpoint_id = cur.fetchone()[0]
        print(f"Created checkpoint with ID: {checkpoint_id}")

        # 2. Insert into predicted_result
        predictions_df = test_df[['pr_link', 'test_case_file_path', 'test_case_name']].copy()
        predictions_df['predicted_result'] = y_pred

        # Aggregate predictions: if any prediction for a test case is 'failed' (1), mark it as 'failed'.
        aggregated_predictions = predictions_df.groupby(['pr_link', 'test_case_file_path', 'test_case_name'])['predicted_result'].max().reset_index()
        aggregated_predictions['checkpoint'] = str(checkpoint_id)

        # Convert predictions to string 'passed'/'failed'
        aggregated_predictions['predicted_result'] = aggregated_predictions['predicted_result'].apply(lambda x: 'failed' if x == 1 else 'passed')

        # Bulk insert using execute_values
        insert_data = [tuple(x) for x in aggregated_predictions.to_numpy()]
        execute_values(
            cur,
            """
            INSERT INTO predicted_result (pr_link, test_case_file_path, test_case_name, predicted_result, checkpoint)
            VALUES %s
            ON CONFLICT (pr_link, test_case_file_path, test_case_name, checkpoint) DO NOTHING
            """,
            insert_data,
            page_size=1000
        )
        
        conn.commit()
        print("Saved predicted results to the database.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while saving to database: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train and evaluate a model.")
    parser.add_argument("--repo_name", type=str, required=True,
                        help="Repository name.")
    parser.add_argument("--data_path", type=str, required=True,
                        help="Path to the training data CSV file.")
    parser.add_argument("--model_path", type=str,
                        default="model/model.joblib", help="Path to save the trained model.")
    parser.add_argument("--metrics_path", type=str, default="metrics/report.json",
                        help="Path to save the evaluation metrics.")

    args = parser.parse_args()

    print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {os.path.basename(__file__)} Start')
    train_and_evaluate(args.repo_name, args.data_path, args.model_path, args.metrics_path)
    print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {os.path.basename(__file__)} End')
