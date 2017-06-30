import sys
import re
from stat_parser import Parser, display_tree
from nltk.tree import *
def main(argv):
    #reading in the document text
    text = ''
    with open("./temp/output"+str(argv[1])+".txt", "r") as inputfile:
        for line in inputfile:
            text += line
    
    #searching for sentences avoiding "."s in the middle like when somebody says U.S.
    h = re.compile('[A-Z][^\.][\s\S]*?[^A-Z][.?!][ $\n]')
    matches = h.findall(text)
    
    #looking for sentences that might qualify
    to_drop = []
    j = re.compile('allocation|fixed income|equity|equities|hybrid|multi-assets?|alternatives|mix|management|assets|investment|managed|invested?', re.IGNORECASE)
    k = re.compile('allocation|fixed income|equity|equities|hybrid|alternatives|multi-assets?', re.IGNORECASE)
    for i in range(len(matches)-1, -1, -1):
        print i
        matches[i] = matches[i].replace('\n', ' ').replace('\r', '')
        print not bool(k.search(matches[i]))
        length = 0 if bool(j.search(matches[i])) else len(j.findall(matches[i]))
        if(length < 2 and (not bool(k.search(matches[i])) or not bool(re.search(r'\d', matches[i])))):
            to_drop.append(i)
    
    for i in to_drop:
        matches.pop(i)
    
    print matches
    #parsing nominated sentences
    # parser = Parser()
    # for i in range(len(matches)):
    #     tree = parser.parse(matches[i])
    #     tree.pretty_print()
    

if __name__ == '__main__': sys.exit(main(sys.argv))