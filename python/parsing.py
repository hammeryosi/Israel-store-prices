import xml.etree.ElementTree as ET
import pandas as pd

# helper: convert xml element to a dictionary
def parseEl(el):
    res = {}
    for it in el.getchildren():
        res[it.tag] = it.text
    return res

def parseXMLFile(filename, fieldName):
	tree = ET.parse(filename)
	elements = tree.iter(fieldName)
	dics = list(map(parseEl, elements))
	return pd.DataFrame(dics)