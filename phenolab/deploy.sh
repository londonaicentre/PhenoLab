#!/bin/bash

ICB=$1
DEPLOY_ENV=$2
echo "Deploying to ICB: $ICB"
echo "Deploying to environment: $DEPLOY_ENV"

if [ "$DEPLOY_ENV" == "prod" ]; then
  APP_TITLE="PhenoLab"
elif [ "$DEPLOY_ENV" == "dev" ]; then
  APP_TITLE="PhenoLab_dev"
else
  echo "Invalid environment specified. Use 'prod' or 'dev'."
  exit 1
fi

# Generate snowflake.yml
echo "Generating snowflake.yml with title: $APP_TITLE"
if [ "$ICB" = "nel" ]; then
  WAREHOUSE="INTELLIGENCE_XS"
elif [ "$ICB" = "sel" ]; then
  WAREHOUSE="INTELLIGENCE_XS"
else
  echo "Unknown ICB: $ICB"
  exit 1
fi
sed -e "s|__APP_TITLE__|$APP_TITLE|g" \
    -e "s|__QUERY_WAREHOUSE__|$WAREHOUSE|g" \
    snowflake.template.yml > snowflake.yml
echo "Generated snowflake.yml:"
cat snowflake.yml

# Generate .env file and deploy to snowflake
if [ "$DEPLOY_ENV" == "prod" ]; then
  echo "Deploying to production environment..."
  echo "DEPLOY_ENV=$DEPLOY_ENV" > .env
  snow streamlit deploy --database "INTELLIGENCE_DEV" --schema "AI_CENTRE_DEV" --replace 
  # Could just be snow streamlit deploy --replace as the default database and schema are specified in the CLI config, 
  # but for clarity
elif [ "$DEPLOY_ENV" == "dev" ]; then
  echo "Deploying to DEV environment..."
  echo "DEPLOY_ENV=$DEPLOY_ENV" > .env
  snow streamlit deploy --database "INTELLIGENCE_DEV" --schema "PHENOLAB_DEV" --replace
fi

# Check if the deployment was successful
if [ $? -eq 0 ]; then
  echo "Deployment succeeded."
else
  echo "Deployment failed."
fi

rm snowflake.yml
rm .env
echo "Temporary snowflake.yml and .env files removed"