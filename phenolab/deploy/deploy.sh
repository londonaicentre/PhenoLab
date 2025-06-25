#!/bin/bash

ENV=$1 
echo "Deploying to environment: $ENV"

if [ "$DEVPROD" == "prod" ]; then
  APP_TITLE="PhenoLab"
else
  APP_TITLE="PhenoLab_dev"
fi

# Generate snowflake.yml
echo "Generating snowflake.yml with title: $APP_TITLE"
sed "s|__APP_TITLE__|$APP_TITLE|g" snowflake.template.yml > snowflake.yml
echo "Generated snowflake.yml:"
cat snowflake.yml

if [ "$ENV" == "prod" ]; then
  echo "Deploying to production environment..."
  snow streamlit deploy --database "INTELLIGENCE_DEV" --schema "AI_CENTRE_DEV" --replace 
  # Could just be snow streamlit deploy --replace as the default database and schema are specified in the CLI config, 
  # but for clarity
else
  echo "Deploying to DEV environment..."
  snow streamlit deploy --database "INTELLIGENCE_DEV" --schema "PHENOLAB_DEV" --replace
fi

if [ $? -eq 0 ]; then
  echo "Deployment succeeded."
else
  echo "Deployment failed."
fi

rm snowflake.yml
echo "Temporary snowflake.yml file removed"