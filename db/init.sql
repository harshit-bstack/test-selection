CREATE TABLE repositories (
    repo_name VARCHAR(255) PRIMARY KEY
);

CREATE TABLE repo_pr_details (
    repo_name VARCHAR(255) NOT NULL REFERENCES repositories(repo_name) ON DELETE CASCADE,
    pr_link VARCHAR(255) NOT NULL,
    date_of_pr TIMESTAMP NOT NULL,
    list_of_authors TEXT [],
    diff_s3_link VARCHAR(255),
    PRIMARY KEY (repo_name, pr_link)
);

CREATE TABLE pr_to_files_changed_mapping (
    pr_link VARCHAR(255) NOT NULL,
    files_paths_changed VARCHAR(255) NOT NULL,
    PRIMARY KEY (pr_link, files_paths_changed)
);

CREATE TABLE pr_to_test_runs_mapping (
    pr_link VARCHAR(255) NOT NULL,
    test_case_file_path VARCHAR(255) NOT NULL,
    test_case_name VARCHAR(255) NOT NULL,
    actual_result VARCHAR(255),
    PRIMARY KEY (pr_link, test_case_file_path, test_case_name)
);

CREATE TABLE predicted_result (
    pr_link VARCHAR(255) NOT NULL,
    test_case_file_path VARCHAR(255) NOT NULL,
    test_case_name VARCHAR(255) NOT NULL,
    predicted_result VARCHAR(255),
    checkpoint VARCHAR(255),
    PRIMARY KEY (pr_link, test_case_file_path, test_case_name, checkpoint)
);

CREATE TABLE checkpoint_details (
    checkpoint SERIAL NOT NULL,
    repo_name VARCHAR(255) NOT NULL REFERENCES repositories(repo_name) ON DELETE CASCADE,
    date_of_checkpoint TIMESTAMP NOT NULL,
    model_path VARCHAR(255) NOT NULL,
    data_path VARCHAR(255) NOT NULL,
    meta_data JSONB,
    PRIMARY KEY (checkpoint, repo_name)
);