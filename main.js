

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

var bunch = false;

program
  .version('0.0.1')
  .option('-t, --time [type]', 'ETL times eg. 20170427 or 2017042721 or 201704272120 ', time)
  .option('-l, --logtype [type]', 'Log Types eg. request or response or impression or click or event')
  .option('-b, --bunch [bool], Perform strategy. true is on five day , false is one pick', bunch)
  .parse(process.argv);






console.log(`start logType: ${program.logtype}, time: ${program.time}, fiveDays: ${program.bunch}`);
const etlExecutor = new EtlExecutor(program.time,program.logtype,program.bunch);
etlExecutor.start();


