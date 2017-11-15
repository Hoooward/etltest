/**
 * Created by chenruibin on 2017/5/25.
 */
const slotidArray = ['59032d3be4b00d54f1646931','59082ecae4b00d54f1646946','59040a2de4b00d54f1646933','59040a54e4b00d54f1646934','5905827fe4b00d54f1646938','59058287e4b00d54f1646939','59058413e4b00d54f164693d'];

const fakeSlotid = function(){
    let timeNumber = new Date().getTime();
    let randomIndex = timeNumber % slotidArray.length;
    return slotidArray[randomIndex];
}


module.exports = {fakeSlotid};