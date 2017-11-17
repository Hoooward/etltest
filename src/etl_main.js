'use strict';
const aws = require('aws-sdk');
const s3 = new aws.S3({
    apiVersion: '2006-03-01'
});
const moment = require('moment-timezone');
const numeral = require('numeral');
const zlib = require('zlib');
const fs = require('fs');

const etls = {
    'click': require('./etl_click'),
}

const bucket = 'com.yodamob.adserver.track';
const maxFileSize = 20 * 1024 * 1024;

var bodyCache = "";


var etlFn = parseline => (data, batchTime) => {

    var beijingTime = moment.tz(batchTime, 'Asia/Shanghai').tz('Asia/Shanghai');
    var batchTimeValue = beijingTime.format('YYYYMMDDHHmm');

    var dataBody = data.Body.toString().split('\n');

    //ETL the Data
    var dataSaveArray = [];

    for (var i = 0; i < dataBody.length; i++) {

        if (dataBody[i] == "")
            continue;

        try {
            var trace = JSON.parse(dataBody[i]);
            var saveRecord = parseline(trace);
            if (saveRecord) {
                saveRecord.batchTime = batchTimeValue;
                dataSaveArray.push(saveRecord);
            }
        } catch (e) {
            console.log(e);
            console.log(dataBody[i]);
            continue;
        }

    }

    return dataSaveArray;
}

var buildEtlPath = function (prefix, time) {
    var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');
    var path = beijingTime.format('YYYYMMDD');
    return `etl_test${prefix}/${path}/`
}

var generateNewLastFileInfo = function (prefix, batchTime) {
    bodyCache = "";
    let lastContentSize = 0;
    var etlTargetFilePath = buildEtlPath(prefix, batchTime) + `${Math.random().toString(36).substr(2)}`;
    return {"bodyPath" : etlTargetFilePath,  "body": bodyCache, "lastContentSize": lastContentSize};
}

var buildPath = function (time) {

    var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');
    var mm = beijingTime.format('mm'); // numeral(parseInt(parseInt(beijingTime.format('mm')) / 10) * 10).format('00');
    var path = beijingTime.format('YYYYMMDD/HH') + mm;
    return path;
}

var buildBody = function (items) {

    var body = "";
    for (var item of items) {
        body = body + JSON.stringify(item) + '\n';
    }
    return body;
}

async function etlExecute(parseline, prefix, times) {

    // 获取 s3 中与当前时间关联的最后一个文件的信息
    let oneBatchTime = times[0];
    let lastBodyInfo = await getLastFileInfo(prefix, oneBatchTime);
    var bodyPath = lastBodyInfo.bodyPath;
    var lastContentSize = lastBodyInfo.lastContentSize;

    var writeBodyCount = 0;
    for (var time of times) {

        console.log('\n\n\n');
        console.log('etlBatchExecute start, batchTime : ', time);

        // 生成原始文件信息路径
        var path = buildPath(time);
        var trackPath = `${prefix}/${path}/`;
        var etl = etlFn(parseline);

        var params_listObject = {
            Bucket: bucket,
            Prefix: trackPath
        };

        try {
            // 获取当前时间片原始数据
            var objectList = await s3.listObjects(params_listObject).promise();

        } catch (err) {
            console.error("fetch s3 file error : ", err);
            throw err;
        }

        console.log("Object File Count from s3 : ", objectList.Contents.length);
        // 遍历当前时间切换的原始数据， 添加到 bodyCache 中.
        for (let object of objectList.Contents) {

            let key = object.Key;
            let params_getObject = {
                Bucket: bucket,
                Key: key,
            };

            console.log('Get Object And ETL : ', params_getObject);
            let traceData = await s3.getObject(params_getObject).promise();

            let items = etl(traceData, time, parseline);
            console.log('ETL Item Count', items.length);

            if (items && items.length > 0) {

                // 将文件信息整合到 bodyCache 中
                let newBody = buildBody(items);

                bodyCache += newBody;
                lastContentSize += Buffer.from(newBody).length;

                // 如果 bodyCache 的大小超过了 maxFileSize，进行上传，并重置 bodyCache.

                if (lastContentSize >= maxFileSize ) {

                    var sourceDir = './sources/'
                    var sourceFilePath = sourceDir + 'baseData'

                    let exists = fs.existsSync(sourceDir)
                    if (!exists) {
                        fs.mkdirSync(sourceDir);
                    }

                    let writeError = fs.appendFileSync(sourceFilePath, bodyCache);

                    if (writeError) {
                        throw  writeError;
                    }

                    bodyCache = '';
                    lastContentSize = 0;
                    writeBodyCount = writeBodyCount + 1;

                    console.log("---------------------")
                    console.log("New body write success!!!");
                    console.log("---------------------")

                    if (writeBodyCount == 5) {

                        console.log('Source file length is fat');
                        console.log('Begin make zip and update...')
                        console.log("---------------------")

                        // 将所有文件写成一个文件
                        var gzip = zlib.createGzip();
                        var inFile = fs.createReadStream(sourceFilePath);
                        var outDir = './gzip/';
                        var outFilePath = outDir + Math.random().toString(36).substr(2) + '.gz';
                        var outFile = fs.createWriteStream(outFilePath);

                        var gzipFinished = new Promise(function (resolve, reject) {
                            inFile.pipe(gzip).pipe(outFile).on('finish', ()=>{
                                resolve('done compressing...');
                            })
                        })

                        let zipResult = await gzipFinished;
                        console.log(zipResult)
                        console.log('new zip file', outFile);

                        writeBodyCount = 0;

                        let resultGzipFile = fs.readFileSync(outFilePath);
                        console.log('resultGzipFile,', resultGzipFile);

                        let params_putObject = {
                            Bucket: bucket,
                            Key: bodyPath,
                            Body: resultGzipFile,
                        };

                        let rs = await s3.putObject(params_putObject).promise();
                        console.log(`ETL Saved To S3 filename ${bodyPath}, rs: `, rs);

                        for (var item of fs.readdirSync(sourceDir)) {
                            fs.unlinkSync(sourceDir + item);
                        }

                        for (var item of  fs.readdirSync(outDir)) {
                            fs.unlinkSync(outDir + item)
                        }

                        // 重置 bodyCache .
                        let newFileInfo = generateNewLastFileInfo(prefix, time)
                        lastContentSize = newFileInfo.lastContentSize;
                        bodyPath = newFileInfo.bodyPath;
                    }

                } else {

                    console.log('bodyCache size => ', lastContentSize);
                }
            }
        }
    }

    // 将剩余不足 100 mb 的数据写入 s3
    if (bodyCache.length != 0 && lastContentSize != 0) {
        // await putBodyCacheToS3(bodyPath);
    }
}

async function putBodyCacheToS3(bodyPath) {
    let params_putObject = {
        Bucket: bucket,
        Key: bodyPath,
        Body: bodyCache,
    };
    let rs = await s3.putObject(params_putObject).promise();
    console.log(`ETL Saved To S3 filename ${bodyPath}, rs: `, rs);
    bodyCache = ""
}

async function getLastFileInfo(prefix, batchTime) {

    //1. 读取旧文件信息
    let etlDirPath= buildEtlPath(prefix, batchTime);
    var etlTargetFilePath = etlDirPath;

    // let newBodyString = buildBody(newItems);
    let params_fetchDirInfo = {
        Bucket: bucket,
        Prefix: etlDirPath,
    };

    var lastContentSize = 0;

    let etlExitingObjects = await s3.listObjects(params_fetchDirInfo).promise();
    if (etlExitingObjects != null && etlExitingObjects.Contents.length != 0) {

        let existingObjectContents = etlExitingObjects.Contents.sort(function (a, b) {
            return a.LastModified.getTime() - b.LastModified.getTime();
        });
        let lastContent = existingObjectContents[existingObjectContents.length - 1];
        let lastContentKey = lastContent.Key;
        let keyArray = lastContentKey.split('\/');
        lastContentSize = lastContent.Size;

        if (lastContentSize < maxFileSize) {
            let params_getLastObject = {
                Bucket: bucket,
                Key: lastContentKey,
            };
            let lastFile = await s3.getObject(params_getLastObject).promise();
            let lastFileBody = lastFile.Body.toString();

            etlTargetFilePath += `${keyArray[keyArray.length - 1]}`;
            bodyCache += JSON.stringify(lastFileBody);

            return {"bodyPath" : etlTargetFilePath,  "body": bodyCache, "lastContentSize": lastContentSize};

        } else {
            return generateNewLastFileInfo(prefix, batchTime);
        }

    } else {
        return generateNewLastFileInfo(prefix, batchTime);
    }
}

function buildHourTimes(hour) {

    var times = [];
    for (var i = 0; i < 6; i++) {
        var t = hour + i + '0';
        times.push(moment.tz(t, 'YYYYMMDDHHmm', 'Asia/Shanghai'));
    }
    return times;
}

function buildDayTimes(day) {

    var times = [];
    for (var i = 0; i < 24; i++) {
        var hour = day + numeral(i).format('00');
        times = times.concat(buildHourTimes(hour));
    }
    return times;
}

function buildTimes(time) {

    var times = [];

    if (time.length == 12) {

        times.push(moment.tz(time, 'YYYYMMDDHHmm', 'Asia/Shanghai'));

    }


    if (time.length == 10) {

        times = times.concat(buildHourTimes(time));

    }

    if (time.length == 8) {
        times = times.concat(buildDayTimes(time));

    }

    return times;

}

const start = async time => {

    console.log('ETL Running Time : ', moment().tz('Asia/Shanghai').format());

    console.log('ETL Input Time : ', time);

    var times = buildTimes(time);
    console.log('ETL batches : ', times.length);

    for (var time of times) {
        await etlBatchExecute(time);
    }

}

class EtlExecutor {
    constructor(time, logtype) {
        this.time = time;
        this.logtype = logtype;
    }

    async start() {

        console.log('ETL Running Time : ', moment().tz('Asia/Shanghai').format());
        console.log('ETL Input Time : ', this.time);

        let times = buildTimes(this.time);
        console.log('ETL batches : ', times.length);

        if (this.logtype === 'offer' ||
          this.logtype === 'record' ||
          this.logtype === 'cost' ||
          this.logtype === 'adSlot') {
            let etlInstance = etls[this.logtype];
            await etlInstance.etlBatchExecute(this.logtype);

        } else if (this.logtype === 'event' ||
          this.logtype === 'CbReceiveLogs'
        ) {
            let etlInstance = etls[this.logtype];
            await etlInstance.initDB();
            for (var time of times) {
                await etlInstance.etlBatchExecute(time);
            }
            await etlInstance.closeDB();
            console.log("event over!")

        } else {
            let parseline = etls[this.logtype].parseline;
            let prefix = etls[this.logtype].prefix;

            if (!parseline || !prefix) {
                throw new Error(`something wrong with param, pls check!\ntime:${this.time}\n$logtype:${this.logtype}\n$etlInstance:${etls[this.logtype]}`);
            }

            await etlExecute(parseline, prefix, times);
            // for (var time of times) {
                // await etlBatchExecute(parseline, prefix)(time);
            // }
        }
    }
}

module.exports = EtlExecutor;