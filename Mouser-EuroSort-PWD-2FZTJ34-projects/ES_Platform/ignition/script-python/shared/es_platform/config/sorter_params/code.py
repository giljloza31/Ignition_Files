
FACE_BACK = "BACK"
FACE_FRONT = "FRONT"
FACE_MID = "MID"
FACE_GATE = "GATE"

SORTER_PARAMS = {
	"MOUSER-ES-A19": {
		"num_of_carriers": 176,
		"multi_lvl": True,			# True => chute "1" and "2"
		"sides": ["A", "B"],
		"divider": 1,
		"gate": True,
		"release_mode": "MANUAL",
		}
	}


def get_sorter_params(systemCode):
	return SORTER_PARAMS.get(systemCode) or {}


def enabled_faces(params):
	d = int(params.get("divider", 0))
	faces = [FACE_BACK]
	if d >= 1:
		faces.append(FACE_FRONT)
	if d >= 2:
		faces.insert(1, FACE_MID)	# BACK, MID, FRONT
	if params.get("gate"):
		faces.append(FACE_GATE)
	return faces


def enabled_chute_levels(params):
	return ["1", "2"] if params.get("multi_lvl") else ["1"]


def enabled_sides(params):
	return params.get("sides") or ["A", "B"]