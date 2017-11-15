/**
 * Created by chenruibin on 2017/5/25.
 */

'use strict';
const fakeSlotid =require('./fakeTool').fakeSlotid;

for(let i=0;i<100000;i++){
    console.log(fakeSlotid());
}