'use strict';
const aws = require('aws-sdk');
const s3 = new aws.S3({
    apiVersion: '2006-03-01'
});
const moment = require('moment-timezone');
const numeral = require('numeral');

const etls = {
    'click': require('./etl_click'),
}

const bucket = 'com.yodamob.adserver.track';

var body = "";
const maxFileSize = 500 * 1024 * 1024;

var etlFn = parseline => (data, batchTime) => {

    //console.log(data.Body.toString());

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

var buildEtlPath = function (time) {
  var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');
  var path = beijingTime.format('YYYYMMDD');
  return path;
}

var buildPath = function (time) {

    //console.log(time);

    var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');

    //console.log(beijingTime.format());

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

const etlBatchExecute = (parseline, prefix) => async batchTime => {
    console.log('etlBatchExecute start, batchTime : ', batchTime);

    var path = buildPath(batchTime);
    var trackPath = `${prefix}/${path}/`;
    var etlPath = `etl_test${prefix}/dt=${path}`;
    var etl = etlFn(parseline);

    var params_listObject = {
        Bucket: bucket,
        /* required */
        Prefix: trackPath
    };

    try {
        // 获取零散数据
        var objectList = await s3.listObjects(params_listObject).promise();

    } catch (err) {

        console.error("fetch s3 file error : ", err);
        throw err;
            }

          console.log("Object File Count from s3 : ", objectList.Contents.length);

          // 成功之后遍历结果
          for (let object of objectList.Contents) {

            let key = object.Key;

            let params_getObject = {
              Bucket: bucket,
              Key: key,
            };

            console.log('Geting Object And ETL : ', params_getObject);
            let traceData = await s3.getObject(params_getObject).promise();

            let items = etl(traceData, batchTime, parseline);

            console.log('ETL Item Count', items.length);


            if (items && items.length > 0) {

              let body = await buildEtlBody(prefix, batchTime, items);

            //   let keyArray = object.Key.split('\/');
            //   let keyLength = keyArray.length;
            //   let fileName = `${etlPath}/${keyArray[keyLength-2]}/${keyArray[keyLength-1]}`;

              let params_putObject = {
                Bucket: bucket,
                Key: body.bodyPath,
                Body: body.body
              };

            // return;

            console.log("bodyInfo: \n")
            // console.log(body)

            let rs = await s3.putObject(params_putObject).promise();

            console.log(`ETL Saved To S3 filename ${body.bodyPath}, rs: `, rs);
        }

    }
}

async function buildEtlBody(prefix, batchTime, newItems) {

  let dirPath = buildEtlPath(batchTime)
  var etlDirPath = `etl_test${prefix}/${dirPath}/`;
  var etlTargetFilePath = etlDirPath;

  let newBodyString = buildBody(newItems);

  let params_fetchDirInfo = {
    Bucket: bucket,
    Prefix: etlDirPath,
  };

  let etlExitingObjects = await s3.listObjects(params_fetchDirInfo).promise();

console.log('ETL existing objects ', etlExitingObjects);

  var writeingToOldFile = false;

  if (etlExitingObjects != null && etlExitingObjects.Contents.length != 0) {

    let existingObjectContents = etlExitingObjects.Contents.sort(function (a, b) {
      return a.LastModified.getTime() - b.LastModified.getTime();
    });
    let lastContent = existingObjectContents[existingObjectContents.length - 1];

    let lastContentSize = lastContent.Size;
    let lastContentKey = lastContent.Key;
    let keyArray = lastContentKey.split('\/');

    if (lastContentSize < maxFileSize) {

      let params_getLastObject = {
        Bucket: bucket,
        Key:lastContentKey,
      };


      // let lastFile = await s3.getObject(params_getLastObject).promise();

      // let lastFileBody = lastFile.Body.toString();
      var newBodyLength = Buffer.from(newBodyString).length;
      console.log("ETL new body length", newBodyLength);

      // 清空 body
      body = "";
      // 如果新内容+旧内容的大小不超过 100 MB
      if (newBodyLength + lastContentSize <= maxFileSize) {
        writeingToOldFile = true;
        etlTargetFilePath += `${keyArray[keyArray.length - 1]}`;
        body += JSON.stringify(lastFileBody);
      }
    }
  }

  if (!writeingToOldFile) {
    etlTargetFilePath += `${Math.random().toString(36).substr(2)}`;
  }

  body += newBodyString;
  return {"bodyPath" : etlTargetFilePath,  "body": body};

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

            for (var time of times) {
                await etlBatchExecute(parseline, prefix)(time);
            }
        }
    }
}

module.exports = EtlExecutor;