*CSV to DynamoDB Node Script*

Chunks the csv and uses `writeBatchItem` dynamodb function in aws-sdk to upload.  

---

To use:

1. Add csv to `./data/` folder
2. Change `file_name` const in script
3. Run.