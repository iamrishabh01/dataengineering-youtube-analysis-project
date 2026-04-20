Bronze Bucket Name - yt-data-pipeline-brozne-ap-south-1-ri
Silver Bucket Name - yt-data-pipeline-silver-ap-south-1-ri
Gold Bucket Name -
yt-data-pipeline-gold-ap-south-1-ri
Script Bucket - yt-data-pipeline-script-ap-south-1-ri

 SNS ARN - arn:aws:sns:ap-south-1:738773506199:yt-data-pipeline-alerts-dev:516ce47a-5ac2-4c46-9d4a-2f7adc793ac6

 Glue Bronze - yt-pipeline-bronze-dev 
 Glue Silver - yt-pipeline-silver-dev 
 Glue Gold - yt-pipeline-gold-dev

--bronze_database yt_pipeline_bronze_dev 
--bronze_table raw_statistics 
--silver_bucket yt-data-pipeline-silver-ap-south-1-dev --silver_database yt_pipeline_silver_dev 
--silver_table clean_statistics

--silver_database yt_pipeline_silver_dev 
--gold_bucket yt-data-pipeline-gold-ap-south-1-dev --gold_database yt_pipeline_gold_dev