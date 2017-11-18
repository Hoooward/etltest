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


const maxFileSize = 100 * 1024 * 1024;
var bodyCache = "";
const sourceDir = './sources/';
const gzipDir = './gzip/';


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

var generateCompressS3FileInfo = function (prefix, batchTime) {
    bodyCache = "";
    let lastContentSize = 0;
    var etlTargetFilePath = buildEtlPath(prefix, batchTime) + `${Math.random().toString(36).substr(2)}` + ".gz";
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

    // 从 s3 中获取与当前时间关联的最后一个文件
    // let oneBatchTime = times[0];
    // let lastBodyInfo = await getLastFileInfo(prefix, oneBatchTime);
    // var bodyPath = lastBodyInfo.bodyPath;

    var lastContentSize = 0;
    var bodyPath = "";

    var needCreateNewS3Path = true

    for (var time of times) {

        console.log('\n\n\n');
        console.log('etlBatchExecute start, batchTime : ', time);

        // 生成原始文件信息路径
        var path = buildPath(time); // : 20071113/0010
        var trackPath = `${prefix}/${path}/`; // : etl_testClick/20071113/
        var etl = etlFn(parseline);

        var params_listObject = {
            Bucket: bucket,
            Prefix: trackPath
        };

        try {
            // 获取当前时间的原始数据列表
            var originalDataList = await s3.listObjects(params_listObject).promise();
        } catch (err) {
            console.error("fetch s3 file error : ", err);
            throw err;
        }

        console.log("Current time dir from s3 has Object : ", originalDataList.Contents.length);

        for (let object of originalDataList.Contents) {

            let key = object.Key;
            let params_getObject = {
                Bucket: bucket,
                Key: key,
            };

            if (needCreateNewS3Path) {
                let newFileInfo = generateCompressS3FileInfo(prefix, times[0]);
                lastContentSize = newFileInfo.lastContentSize;
                bodyPath = newFileInfo.bodyPath;

                console.log('Create new push path => ', bodyPath);
                needCreateNewS3Path = false;
            }

            console.log('Get current time dir objects And ETL : ', params_getObject);
            let traceData = await s3.getObject(params_getObject).promise();

            let items = etl(traceData, time, parseline);
            console.log('Fetch etl item count', items.length);

            if (items && items.length > 0) {

                let newBody = buildBody(items);

                var needClearOldBodyFile = lastContentSize >= maxFileSize
                await prepareGenerateBodyFile(needClearOldBodyFile)

                //将新数据写入本地
                var sourceFilePath = sourceDir + 'baseData'
                let writeError = fs.appendFileSync(sourceFilePath, newBody);

                if (writeError) {
                    throw  writeError;
                }

                lastContentSize += Buffer.from(newBody).length;
                console.log("New body write success. new file size => ", lastContentSize);

                if (lastContentSize >= maxFileSize) {

                    console.log('Body file length is fat');
                    console.log('Begin make zip and update...')
                    console.log("---------------------")

                    await pushBodyCacheFileToS3(bodyPath)

                    needCreateNewS3Path = true;
                }
            }
        }
    }

    if (lastContentSize != 0) {
        await pushBodyCacheFileToS3(bodyPath);
    }
}

async function pushBodyCacheFileToS3(bodyPath) {

    await prepareMakeCompress();

    // 将所有文件写成一个文件
    var sourceFilePath = sourceDir + 'baseData'
    var gzip = zlib.createGzip();
    var inFile = fs.createReadStream(sourceFilePath);
    var gzFilePath = gzipDir + 'zipData.gz';
    var gzFile = fs.createWriteStream(gzFilePath);

    var gzipFinished = new Promise(function (resolve, reject) {
        inFile.pipe(gzip).pipe(gzFile).on('finish', () => {
            resolve('Done compressing...');
        })
    })

    let gzResult = await gzipFinished;
    console.log('Compressing result', gzResult)

    let resultGzipFile = fs.readFileSync(gzFilePath);
    console.log('New compress file,', resultGzipFile);

    let params_putObject = {
        Bucket: bucket,
        Key: bodyPath,
        Body: resultGzipFile,
    };

    console.log('Beginning push etl gz file to s3...')
    let rs = await s3.putObject(params_putObject).promise();
    console.log(`ETL saved to S3 filename ${bodyPath}, rs: `, rs);
}

async function prepareGenerateBodyFile(needClearOldBodyFile) {

    let sourceExists = fs.existsSync(sourceDir)
    if (sourceExists) {
        if (needClearOldBodyFile) {
            await deleteOldDataFrom(sourceDir)
        }
    } else {
        fs.mkdirSync(sourceDir);
        console.log('Created dir =>', sourceDir)
    }
}

async function prepareMakeCompress() {

    let gzipExists = fs.existsSync(gzipDir)
    if (gzipExists) {
        await deleteOldDataFrom(gzipDir)
    } else {
        fs.mkdirSync(gzipDir)
        console.log('Created dir =>', gzipDir)
    }
}

async function deleteOldDataFrom(path) {

    let files = fs.readdirSync(path)
    if (files && files.length > 0) {
        for (var file of files) {
            fs.unlinkSync(path + file);
            console.log('Delete file path =>', path + file);
        }
    }
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

    // 获取文件夹中所有内容
    let etlExitingObjects = await s3.listObjects(params_fetchDirInfo).promise();
    if (etlExitingObjects != null && etlExitingObjects.Contents.length != 0) {

        // 排序
        let existingObjectContents = etlExitingObjects.Contents.sort(function (a, b) {
            return a.LastModified.getTime() - b.LastModified.getTime();
        });

        let lastContent = existingObjectContents[existingObjectContents.length - 1];
        let lastContentKey = lastContent.Key;
        let keyArray = lastContentKey.split('\/');
        lastContentSize = lastContent.Size;

        if ((lastContentSize * 5) < maxFileSize) {
            let params_getLastObject = {
                Bucket: bucket,
                Key: lastContentKey,
            };

            // 获取最后一个文件的内容
            let lastFile = await s3.getObject(params_getLastObject).promise();

            // 最后一个应该是压缩好的格式
            // TODO: - 需要解压缩

            let lastFileBody = lastFile.Body.toString();

            etlTargetFilePath += `${keyArray[keyArray.length - 1]}`;
            bodyCache += JSON.stringify(lastFileBody);

            return {"bodyPath" : etlTargetFilePath,  "body": bodyCache, "lastContentSize": lastContentSize};

        } else {
            return generateCompressS3FileInfo(prefix, batchTime);
        }

    } else {
        return generateCompressS3FileInfo(prefix, batchTime);
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