
const parseline = trace => {
    if (trace['logtype'] == 'c') {
        return {
            logid:trace['logid'],
            reqid:trace['reqid'],
            slotid:trace['slotid'],//slotid:trace['slotid'],
            ydofid: trace['ydofid'],
            yodasc: trace['yodasc'],
            ydosid: trace['ydosid'],
            ydogid: trace['ydogid'],
            ydckid: trace['ydckid'],
            country:trace['icc'],
            userid: trace['userid'],
            ip: trace['ip'],
            idfa: trace['idfa'],
            ts: trace['ts']
        };
    }else{
        return null;
    }
}

exports.parseline = parseline;
exports.prefix = "clicktrack";