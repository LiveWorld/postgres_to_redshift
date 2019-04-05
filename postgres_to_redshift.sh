#!/bin/bash

export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://<postgresql_database>/<database>'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://<user>:<password>@<redshift_url>:5439/<database>'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='public'
export S3_DATABASE_EXPORT_ID='<INSERT ID HERE>'
export S3_DATABASE_EXPORT_KEY='<INSERT KEY>'
export S3_DATABASE_EXPORT_BUCKET='<bucket>'
export AWS_REGION='<region>'
export REDSHIFT_PARAMETERS='ACCEPTINVCHARS MAXERROR 1000 COMPUPDATE ON'
export POSTGRES_TO_REDSHIFT_INCREMENTAL=true
export POSTGRES_TO_REDSHIFT_DRY_RUN=false
export REDSHIFT_INCLUDE_TABLES='table-to-include1,table-to-include2'
export TABLES_ALWAYS_FULL_IMPORT='table-to-alaways-full-import1,table-to-alaways-full-import2'
export SLACK_CHANNEL='#notifications'
export SLACK_USERNAME='Redshift'
export SLACK_ICON_EMOJI=':redshift:'
export SLACK_WEBHOOK_URL='<slack webhook url>'

/usr/bin/time /usr/local/rbenv/shims/ruby /usr/local/rbenv/shims/postgres_to_redshift
