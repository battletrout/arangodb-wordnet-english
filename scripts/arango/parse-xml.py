import xml.etree.ElementTree as ET

#********Make this OS-agnostic before deploy******
tree = ET.parse('wn.xml')
root = tree.getroot()

print(root[0].tag)
for child in root[0][0:5]:
    print(child.tag, child.attrib)
    for grandchild in child:
        print(grandchild.tag, grandchild.attrib)

print(root[0][0].items())
print(root[0][0][0].tag, root[0][0][0].attrib)
