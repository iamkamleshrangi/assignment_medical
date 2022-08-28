import yaml

with open('/Users/kamleshkumarrangi/workspace/assignment_medical/lib/config.yaml', 'r') as f:
    doc = yaml.load(f)

def handler(tree,node):
    return doc[tree][node]
