import subprocess
import re
import tabula
import nltk
from os import remove as DeleteFile
from os import listdir
from os.path import isfile, join
import pandas as pd
from PyPDF2 import PdfFileReader

#regexs to distinguish dollar values, asset classes and percent values
re_digits = "\d+(?:[ ,\.-]?\d+){0,3}"
re_currency = "(?:USD|US dollars?|dollars?|[$¢£¤¥֏؋৲৳৻૱௹฿៛\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6])"
re_words = "(?:(?:million|thousand|hundred|billion|M|B|T|K|m|bn|tn)(?![a-z]))"

re_headings = '(?:Fund mix|Assets|Group investments|Investment allocation|Fund management|Investment management|assets|sset class|aum)'
re_classes = '(?:equities|equity|fixed income|alternatives?|multi[ -]assets?|hybrid|cash management|money markets?|bonds?|stocks?)'
re_money = currency+'?\s?'+digits+'\s?'+words+'?\s?'+currency+'?(?![%\d])'
re_percent = digits+'%'

def main():
	files = [f for f in listdir('./docs') if isfile(join('./docs', f))]
	for file in files:
		doc_search(file)

class Document():
	def __init__(self, file):
		self.title = check_title(file)
		self.OFFSET = int(file[0:2])
		self.path = "./docs/" + file
		self.pdf_reader = PdfFileReader(open(self.path, "rb"))
		self.page_dict = {}
		
	
	def extract():
		load_doc()
		page_filter()
		table_extract(self.OFFSET)
		text_extract()
		clean_up()

	def check_title(file):
		#checking if name meets standards for later printing use, and 
		pattern = re.compile("^\d{2,3}\. [\S\s]+? \([A-Za-z ]+\) \d{4} ?Q?[1-4]?")
		match = pattern.match(file)
		if (match):
		    title = match.string
		    print(title)
		else:
			print(file, " doesn't meet naming conventions")
		return title

	def load_doc():
		try:
			self.pdf_reader.decrypt('')
		except:
			pass
		pages = self.pdf_reader.getNumPages()
		
		# I load the pages into the document object.
		for page in range(pages):
		    print("p",page, " is loading")
		    self.page_dict[self.OFFSET+page] = ''
		    #converting to text using python2 script, which output to ./temp
		    cmd = ["python2", "./pdf2txt/tools/pdf2txt.py", "-o", "./temp/output"+str(self.OFFSET+page)+".txt", "-p", str(self.OFFSET+page),"-t", "text", path]
		    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		    temp, error = p.communicate()
		    if error:
		        print(error.decode("utf-8"))
		    #reading in files to get text from ./temp   
		    with open("./temp/output"+str(page)+".txt", "r") as page_output:
		        for line in page_output:
		            self.page_dict[self.OFFSET+page] += line.strip().lower() + ' '
		    page_output.close()
		print("Document loaded")

	def page_filter():
		# Once loaded, the document has to go through a basic page scoring step. If the page doesn't contain Asset/Investment allocation or the two words equity and fixed income, then the page is thrown away.
		page_nomination = []
		cases = "(?:"+re_headings+"|"+re_classes+")[\s\S]*""(?:"+re_headings+"|"+re_classes+")"
		p = re.compile(cases)
		for page in self.page_dict:
		    matches = p.findall(self.page_dict[page])
		    if matches:
		        page_nomination.append(page)
		    print(matches)
		#taking subset of original document object
		self.page_dict = {k: self.page_dict[k] for k in page_nomination}

	def table_extract(pages_to_extract = self.page_dict):
		# To analyze the pages, first I'll try to convert it to a table and then I'll validate the first table column entries (equity, fixed income, and optionally multi asset and alternatives) I'll also try to look for columns indicating years.
		results = []
		try:
		    page = self.pdf_reader.getPage(0)
		except:
		    self.pdf_reader.decrypt('')
		    page = self.pdf_reader.getPage(0)

		top = page.mediaBox.upperRight[1]
		right = page.mediaBox.upperRight[0]
		bottom = page.mediaBox.lowerLeft[1]
		left = page.mediaBox.lowerLeft[0]
		areas = [[top, left, bottom, right],
		         [top, left, (bottom+top)/2, right],[(bottom+top)/2, left, bottom, right],
		         [top, left, (bottom+top)/2, (right+left)/2],[top, (right+left)/2, (bottom+top)/2, right],
		         [(bottom+top)/2, (right+left)/2, bottom, right],[(bottom+top)/2, left, bottom, (right+left)/2]
		        ]

		k = re.compile(re_classes, re.IGNORECASE)
		j = re.compile(re_currency, re.IGNORECASE)
		df = pd.DataFrame([], columns=['equities', 'equities %', 'fixed income', 'fixed income %', 'alternative', 'alternative %', 'multi-asset','multi-asset %', 'cash management', 'cash management %'], index=[title])

		for page in pages_to_extract:
		    for  area in areas:
		        try:
		            table = tabula.read_pdf(path, pages=page, area = area)
		            p = table.T.reset_index().T
		            p = p.apply(lambda x: x.astype(str).str.lower())
		            for row_ind, row in p.iterrows():
		                search = True
		                for col_ind, col in row.iteritems():
		                    if search: 
		                        if (len(k.findall(col))>1):
		                            break
		                        if bool(k.search(col)):
		                            asset = k.search(col).group(0)
		                            search = False
		                            
		                            if bool(j.search(col)):
		                                df[asset] = j.search(col).group(0)
		                                break
		                    else:
		                        if bool(j.search(col)):
		                            df[asset] = j.search(col).group(0)
		                            break
		                        
		            if (df.T.count()[0] > 2):
		                print('Result found, saving p', page)
		                results.append(df.T)
		                break
		            else:
		                print('Invalid values')            
		        except Exception as inst:
		            print("Unsuccessful table search: page", page, inst)



	def text_extract():
		tokenizer = nltk.tokenize.RegexpTokenizer(r''+re_percent+'|'+re_classes+'|'+re_money)
		regexp_tagger = nltk.tag.RegexpTagger(
		    [(r''+re_classes, 'CL'),
		     (r''+re_money, 'NU'),
		     (r''+re_percent, 'PC'),     
		    ])
		# If no tables are found, we continue trying to text mine the expected results. This Python2 script looks for conversational sentences.
		for page in self.page_dict:
		    text = ''
		    with open("./temp/output"+str(page)+".txt", "r") as inputfile:
		        for line in inputfile:
		            text += line

		    h = re.compile('[A-Z][^\.][\s\S]*?[^A-Z][.?!](?![A-Z][^a-z]|\d)')
		    matches = h.findall(text)
		    print(matches)
		    #looking for sentences that might qualify
		    to_drop = []
		    j = re.compile('allocation|fixed income|equity|equities|hybrid|multi-assets?|alternatives|mix|management|assets|investment|managed|invested?', re.IGNORECASE)
		    k = re.compile('allocation|fixed income|equity|equities|hybrid|alternatives|multi-assets?', re.IGNORECASE)
		    for i in range(len(matches)-1, -1, -1):
		        matches[i] = matches[i].replace('\n', ' ').replace('\r', '')
		        length = 0 if bool(j.search(matches[i])) else len(j.findall(matches[i]))
		        if(length < 2 and (not bool(k.search(matches[i])) or not bool(re.search(r'\d', matches[i])))):
		            to_drop.append(i)

		    for i in to_drop:
		        matches.pop(i)

		    print(matches)


		
		#tokenize into the useful tokens and tag them whether they're nubmers or classes blabla
		tokens = tokenizer.tokenize(matches[0])
		tags = regexp_tagger.tag(tokens)
		print(tags)

		#we only use the sentence if the number of asset classes match up with the number values. Otherwise it's just a mess
		pc = nu = cl = 0
		for i in tags:
		    if i[1] == 'PC':
		        pc += 1
		    elif i[1] == 'NU':
		        nu += 1
		    elif i[1] == 'CL':
		        cl += 1

		df = pd.DataFrame([], columns=['equities', 'equities %', 'fixed income', 'fixed income %', 'alternative', 'alternative %', 'multi-asset','multi-asset %', 'cash management', 'cash management %'], index=[title])

		per = num = cla = None
		if (pc == cl == nu):
		    for i in tags:        
		        if i[1] == 'PC':
		            if per == None:
		                per = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if i[1] == 'NU':
		            if num == None:
		                num = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if i[1] == 'CL':
		            if cla == None:
		                cla = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if per and num and cla:
		            df[cla] = num
		            df[cla+' %'] = per
		            per = num = cla = None
		            
		elif (cl == pc):
		    for i in tags:        
		        if i[1] == 'PC':
		            if per == None:
		                per = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if i[1] == 'CL':
		            if cla == None:
		                cla = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if per and cla:
		            df[cla+' %'] = per
		            cla = per = None
		elif (cl == nu):
		    for i in tags:        
		        if i[1] == 'NU':
		            if num == None:
		                num = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if i[1] == 'CL':
		            if cla == None:
		                cla = i[0]
		            else:
		                print("Values didn't match up. Sentence not finished.")
		                break
		        if num and cla:
		            df[cla] = num
		            num = cla = None
		print(df.T)         

	def clean_up:
		files = [f for f in listdir('./temp') if isfile(join('./temp', f))]
		for file in files:
			DeleteFile(file)