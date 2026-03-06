from cosmos import DbtDag, ProjectConfig, ProfileConfig

# dbt_dag = DbtDag(
#     project_config=ProjectConfig("/path/to/dbt_project"),
#     profile_config=ProfileConfig(
#         profile_name="my_motherduck_project",
#         target_name="dev",
#         profiles_yml_filepath="/path/to/profiles.yml"
#     ),
#     schedule_interval='@daily',
#     start_date=datetime(2026, 3, 1),
#     dag_id="dbt_motherduck_dag",
# )