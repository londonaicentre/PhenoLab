# onelondon_snowflake_datascience
Data Science Monorepo containing reproducible code/pipelines for population health analytics and machine learning in OneLondon Snowflake

## Project Structure

`onelondon_snowflake_datascience` is set-up as a monorepo.

- `/src` phmlondon package that is imported into projects, containing utils for interacting with snowflake, terminology server, and other helper classes.
- `/projects` individual projects that contain data science pipelines, not part of the package.

```
|src/
|--phmlondon/
|----__init__.py
|----onto_utils.py
|----snow_utils.py
|
|projects/
|--.env.example
|--project_a/
|----requirements.txt
|----.env
|--project_b/
|----requirements.txt
|----.env
|
|pyproject.toml
|README.md
``` 
