const fs = require('fs'),
			aws = require('aws-sdk'),
			async = require('async'),
			parse = require('csv-parse'),
			Confirm = require('prompt-confirm');

aws.config.update({region: 'us-east-1'});
const file_name = './data/20190403.csv',
			colors = ["\x1b[40m","\x1b[41m","\x1b[42m","\x1b[43m","\x1b[44m","\x1b[45m","\x1b[46m","\x1b[47m"],
			db = new aws.DynamoDB();

/* function sync()
	+ begins the process of converting the csv into a 
	data stream then piped into a csv parser
*/
const sync = function(){
	const handle = parse({columns: true, delimiter: ','},
		function(err, data){
			var chunks = [], chunk = [], success = 0, failures = 0, current;
			for (let i = 0; i < data.length; i++){
				chunk.push({
					PutRequest: {
						Item: {
							"email": { "S": data[i].email},
							"name": { "S": data[i].name }
						}
					}
				});
				if (i % 25 === 0){
					chunks.push(chunk);
					chunk = [];
				}
			}
			async.eachOf(chunks, (shard, key, cb)=>{
				// console.log('\x1b[33m', `Attempting to upload shard #${key+1} out of ${chunks.length}`);
				db.batchWriteItem({ RequestItems: { "TABLE_NAME": shard } }, function(err,rsp){
					current = key + 1;
					if (err){
						failures++;
						console.log('\x1b[31m', `\tFAILED upload shard #${current} out of ${chunks.length}`, err);
					} else {
						success++;
						console.log('\x1b[32m', `\tUploaded shard #${current} out of ${chunks.length}`);
					}
					cb();
				});
			},(err)=>{
				if (err){
					console.log('\x1b[31m','FAILED UPLOAD','\x1b[32m',`${success} successes `,'\x1b[31m',`${failures} failures`);
				} else {
					console.log('\x1b[37m','COMPLETE: ','\x1b[32m',`${success} successes `,'\x1b[31m',`${failures} failures`);
				}
			});	
		});
	fs.createReadStream(file_name).pipe(handle);
};

(new Confirm(`Do you want to Import ${file_name} to DynamoDB?`))
	.ask((choice)=>{
		if (choice)
			sync(); 
		else
			console.log('Done.');
	});
