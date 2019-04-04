#!/bin/bash

export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://<postgresql_database>/<database>'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://<user>:<password>@<redshift_url>:5439/<database>'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='public'
export S3_DATABASE_EXPORT_ID='<INSERT ID HERE>'
export S3_DATABASE_EXPORT_KEY='<INSERT KEY>'
export S3_DATABASE_EXPORT_BUCKET='redshift-quicksight'
export AWS_REGION='us-east-1'
export REDSHIFT_PARAMETERS='ACCEPTINVCHARS MAXERROR 1000 COMPUPDATE ON'
export POSTGRES_TO_REDSHIFT_INCREMENTAL=true
export POSTGRES_TO_REDSHIFT_DRY_RUN=false
export REDSHIFT_INCLUDE_TABLES='authors,authors_content_tags,cases,cases_content_tags,content_posts,content_posts_content_tags,content_posts_rules,content_tags,customers,events,groups,notifications,rejection_reasons,response_templates,rules,systems,systems_rejection_reasons,systems_response_templates,users'
export TABLES_ALWAYS_FULL_IMPORT='authors_content_tags,content_posts_content_tags,cases_content_tags,systems_rejection_reasons,systems_response_templates'
export SLACK_CHANNEL='#devops'
export SLACK_USERNAME='Redshift'
export SLACK_ICON_EMOJI=':postgresql:'
export SLACK_WEBHOOK_URL='<slack webhook url>'

#/usr/bin/time /bin/rbenv_wrapper /usr/local/rbenv/shims/postgres_to_redshift
/usr/bin/time /usr/local/rbenv/shims/ruby /usr/local/rbenv/versions/2.4.2/lib/ruby/gems/2.4.0/gems/postgres_to_redshift-0.3.0/bin/postgres_to_redshift
