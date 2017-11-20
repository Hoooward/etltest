

const program = require('commander');
const moment = require('moment-timezone');
const numeral = require('numeral');
const EtlExecutor = require('./src/etl_main');


var time = (() => {
    let time = moment().tz('Asia/Shanghai').add(-20, 'minute');
    let day = time.format('YYYYMMDDHH');
    let mm = parseInt(parseInt(time.format('mm')) / 10);
    return `${day}${mm}0`;
})();

console.log(time);

program
  .version('0.0.1')
  .option('-t, --time [type]', 'ETL times eg. 20170427 or 2017042721 or 201704272120 ')
  .option('-l, --logtype [type]', 'Log Types eg. request or response or impression or click or event')
  .parse(process.argv);

console.log(`start logType: ${program.logtype}, time: ${program.time}`);
const etlExecutor = new EtlExecutor(program.time,program.logtype);
etlExecutor.start();