# shared/es_platform/commands/receipt_api.py
# Convenience functions for Perspective/UI to query command receipts

from shared.es_platform.commands.receipt_store import COLLECTION_COMMANDS


class ReceiptAPI(object):
	def __init__(self, mongo, systemCode=None):
		self.mongo = mongo
		self.systemCode = systemCode

	def get(self, commandId):
		"""
		Get a single receipt by commandId.
		"""
		if not commandId:
			return None
		return self.mongo.find_one(COLLECTION_COMMANDS, {"_id": str(commandId)})

	def recent(self, limit=50, systemCode=None, status=None, chuteId=None, carrierId=None, requestedBy=None, authorizedBy=None, eventType=None):
		"""
		List recent receipts (newest first).

		NOTE:
		- If your MongoProxy doesn't support sort/limit inside find(), we fall back to a simple find + slice.
		"""
		sys_code = systemCode or self.systemCode
		f = {}
		if sys_code:
			f["systemCode"] = sys_code
		if status:
			f["status"] = str(status)
		if chuteId:
			f["chuteId"] = str(chuteId)
		if carrierId is not None:
			f["carrierId"] = int(carrierId)
		if requestedBy:
			f["requestedBy"] = str(requestedBy)
		if authorizedBy:
			f["authorizedBy"] = str(authorizedBy)
		if eventType:
			f["eventType"] = str(eventType)

		lim = int(limit)

		# Best case: proxy supports find(sort=..., limit=...)
		try:
			rows = self.mongo.find(COLLECTION_COMMANDS, f, sort=[("createdAtEpoch", -1)], limit=lim) or []
			return rows
		except:
			pass

		# Fallback: raw find and slice (may be slower on huge collections)
		try:
			rows = self.mongo.find(COLLECTION_COMMANDS, f) or []
			# sort newest first locally if possible
			try:
				rows.sort(key=lambda r: int(r.get("createdAtEpoch") or 0), reverse=True)
			except:
				pass
			return rows[:lim]
		except:
			return []

	def failed(self, limit=50, systemCode=None):
		return self.recent(limit=limit, systemCode=systemCode, status="FAILED")

	def pending(self, limit=50, systemCode=None):
		# queued or sent not yet ack'd
		sys_code = systemCode or self.systemCode
		out = []
		for st in ("QUEUED", "SENT"):
			out.extend(self.recent(limit=limit, systemCode=sys_code, status=st))
		# keep newest first and cap
		try:
			out.sort(key=lambda r: int(r.get("createdAtEpoch") or 0), reverse=True)
		except:
			pass
		return out[:int(limit)]
		
#		
#from shared.foundation.mongo.proxy import MongoProxy
#from shared.es_platform.commands.receipt_api import ReceiptAPI
#
#mongo = MongoProxy(connector="MongoWCS", gateway_project="ES_Platform", handler_name="MongoProxy")
#
#api = ReceiptAPI(mongo, systemCode="MOUSER-ES-C1")
#
## Get one receipt
#r = api.get("MOUSER-ES-C1-1734225567123-abcd")
#print(r)
#
## Last 25 commands
#recent = api.recent(limit=25)
#print(len(recent))
#
## Failed commands
#failed = api.failed(limit=20)
#print(len(failed))
#
## Pending (queued or sent)
#pending = api.pending(limit=20)
#print(len(pending))
#
## Filter by chute
#by_chute = api.recent(limit=25, chuteId="DST-0012-1-1-A")
#print(len(by_chute))