package kvraft

func (kv *KVServer) processReq(op *Op) {
	uuid := op.OpUUID
	client, opID := SplitUUID(uuid)
	var res string
	switch op.MethodType {
	case "Get":
		key := op.MethodArg[0]
		if s, ok := kv.Map[key]; !ok {
			res = ""
		} else {
			res = s
		}
	case "Put":
		key, value := op.MethodArg[0], op.MethodArg[1]
		kv.Map[key] = value
		res = value
	case "Append":
		key, value := op.MethodArg[0], op.MethodArg[1]
		if s, ok := kv.Map[key]; ok {
			kv.Map[key] = s + value
			res = s + value
		} else {
			kv.Map[key] = value
			res = value
		}
	}
	kv.requestRes[client] = res
	kv.History[client] = opID
	if kv.isReady[uuid] != nil {
		close(kv.isReady[uuid])
	}
	logger.Infof("KVServer %v execute op %v succeed res is %v", kv.me, op, res)
}
