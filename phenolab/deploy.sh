#!/bin/bash

ICB=$1
DEPLOY_ENV=$2
echo "Deploying to ICB: $ICB"
echo "Deploying to environment: $DEPLOY_ENV"

if [ "$DEPLOY_ENV" != "prod" ] && [ "$DEPLOY_ENV" != "dev" ]; then
  echo "Invalid environment specified. Use 'prod' or 'dev'."
  exit 1
fi

# gets all config values from the yaml
read_config() {
  local key=$1
  python3 -c "import yaml; config = yaml.safe_load(open('configs/${ICB}_icb_${DEPLOY_ENV}.yml')); keys = '${key}'.split('.'); value = config; [value := value.get(k, '') for k in keys]; print(value)"
}

if [ ! -f "configs/${ICB}_icb_${DEPLOY_ENV}.yml" ]; then
  echo "Error: Config file configs/${ICB}_icb_${DEPLOY_ENV}.yml not found"
  echo "Available ICBs: nel, sel"
  exit 1
fi

# get deployment settings
APP_TITLE=$(read_config "deployment.app_title")
WAREHOUSE=$(read_config "deployment.warehouse")
DATABASE=$(read_config "deployment.database")
SCHEMA=$(read_config "deployment.schema")

echo "Using configuration:"
echo "  App Title: $APP_TITLE"
echo "  Warehouse: $WAREHOUSE"
echo "  Database: $DATABASE"
echo "  Schema: $SCHEMA"

# Generate snowflake.yml
echo "Generating snowflake.yml with title: $APP_TITLE"
sed -e "s|__APP_TITLE__|$APP_TITLE|g" \
    -e "s|__QUERY_WAREHOUSE__|$WAREHOUSE|g" \
    snowflake.template.yml > snowflake.yml
echo "Generated snowflake.yml:"
cat snowflake.yml

# for first time run, creates relevant schema
echo "Ensuring schema $DATABASE.$SCHEMA exists..."
snow sql --connection "${ICB}_icb" --query "CREATE SCHEMA IF NOT EXISTS $DATABASE.$SCHEMA;"

# Generate .env file and deploy to snowflake
echo "Deploying to $DEPLOY_ENV environment..."
echo "DEPLOY_ENV=$DEPLOY_ENV" > .env
snow streamlit deploy --connection "${ICB}_icb" --database "$DATABASE" --schema "$SCHEMA" --replace

# Check if the deployment was successful
if [ $? -eq 0 ]; then
  echo "Streamlit deployment succeeded."

  # runs setup.py to load all tables and definitions
  echo "Running setup.py to load definitions and configurations..."
  python setup.py ${ICB}_icb $DEPLOY_ENV 

  if [ $? -eq 0 ]; then
    echo "Setup completed successfully."
    echo "Deployment fully completed."
  else
    echo "Setup failed."
    exit 1
  fi
else
  echo "Streamlit deployment failed."
  exit 1
fi

rm snowflake.yml
rm .env
echo "Temporary snowflake.yml and .env files removed"