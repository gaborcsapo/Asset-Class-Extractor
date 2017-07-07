import subprocess
import re
import tabula
import nltk
from os import getcwd
from os import remove as DeleteFile
from os import listdir
from os.path import isfile, join
import pandas as pd
from PyPDF2 import PdfFileReader
import csv

#regexs to distinguish dollar values, asset classes and percent values
re_digits = "\d+(?:[ ,\.-]?\d+){0,3}"
re_currency = "(?:USD|US dollars?|dollars?|[$¢£¤¥֏؋৲৳৻૱௹฿៛\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6])"
re_words = "(?:(?:million|thousand|hundred|billion|M|B|T|K|m|bn|tn)(?![a-z]))"

re_context = '(?:mix|management|assets|investment|managed|invested)?'
re_headings = '(?:Fund mix|Assets|Group investments|Investment allocation|Fund management|Investment management|assets|sset class|aum)'
re_classes = '(?:equities|equity|fixed income|alternatives?|multi[ -]assets?|hybrid|cash management|money markets?|bonds?|stocks?)'
re_money = re_currency+'?\s?'+re_digits+'\s?'+re_words+'?\s?'+re_currency+'?(?![%\d])'
re_percent = re_digits+'%'

HEADER = ['name', 'equity', 'equity%', 'fixed income', 'fixed income%', 'alternative', 'alternative%', 'multi-asset','multi-asset%', 'money market', 'money market%', 'file']

def main():
	with open('results.csv', 'w') as output:
		writer = csv.writer(output, dialect='excel')
		writer.writerow(HEADER)
	output.close()
	with open('candidates.csv', 'w') as output:
		writer = csv.writer(output, dialect='excel')
		writer.writerow(HEADER)
	output.close()
	files = [f for f in listdir('./docs') if isfile(join('./docs', f))]
	for file in files:
		a = Document(file)
		a.extract()

class Document():
	def __init__(self, file):
		self.path = "./docs/" + file
		self.title = None
		self.OFFSET = 0
		self.pdf_reader = PdfFileReader(open(self.path, "rb"))
		self.page_dict = {}
		
	
	def extract(self):
		self.check_title(self.path)
		self.load_doc()
		self.page_filter()

		candidates = []
		candidates += self.table_extract([self.OFFSET]) #self.table_extract(self.page_dict)
		candidates += self.text_extract()
		if (len(candidates) == 0):
			print('No candidates found in', self.title)
			return
		best = self.evaluate(candidates)
		self.write_csv([best], True)
		self.write_csv(candidates, False)
		self.clean_up()

	def check_title(self, file):
		#checking if name meets standards for later printing use, and 
		pattern = re.compile("^\d{2,3}\. [\S\s]+? \([A-Za-z ]+\) \d{4} ?Q?[1-4]?")
		match = pattern.match(file[7:])
		if (match):
		    title = match.string
		    print(title)
		else:
			print(file, " doesn't meet naming conventions")
		self.title = title
		self.OFFSET = int(file[7:9])

	def load_doc(self):
		print("####LOADING PDF####")
		
		try:
			self.pdf_reader.decrypt('')
		except:
			pass
		pages = 3 #self.pdf_reader.getNumPages()
		
		# I load the pages into the document object.
		for page in range(pages):
		    print("p",page, " is loading")
		    self.page_dict[self.OFFSET+page] = ''
		    
		    #converting to text using python2 script, which output to ./temp
		    cmd = ["python2", "./pdf2txt/tools/pdf2txt.py", "-o", "./temp/output"+str(self.OFFSET+page)+".txt", "-p", str(self.OFFSET+page),"-t", "text", self.path]
		    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		    temp, error = p.communicate()
		    if error:
		        print(error.decode("utf-8"))
		    
		    #reading in files to get text from ./temp   
		    with open("./temp/output"+str(self.OFFSET+page)+".txt", "r") as page_output:
		        for line in page_output:
		            self.page_dict[self.OFFSET+page] += line.strip() + ' '
		    page_output.close()

		print("Document loaded")

	# Once loaded, the document has to go through a basic page scoring step. If the page doesn't contain Asset/Investment allocation or the two words equity and fixed income, then the page is thrown away.
	def page_filter(self):
		page_nomination = []
		cases = "(?:"+re_headings+"|"+re_classes+")[\s\S]*""(?:"+re_headings+"|"+re_classes+")"
		p = re.compile(cases, re.IGNORECASE)
		for page in self.page_dict:
		    matches = p.findall(self.page_dict[page])
		    if matches:
		        page_nomination.append(page)
		#taking subset of original document object
		self.page_dict = {k: self.page_dict[k] for k in page_nomination}

	# To analyze the pages, first I'll try to convert it to a table and then I'll validate the first table column entries (equity, fixed income, and optionally multi asset and alternatives) I'll also try to look for columns indicating years.
	def table_extract(self, pages_to_extract):
		print("####TABLE EXTRACTION####")
		results = []
		try:
		    page = self.pdf_reader.getPage(0)
		except:
		    self.pdf_reader.decrypt('')
		    page = self.pdf_reader.getPage(0)

		#table extraction sometimes works better if we only feed in a small portion of the page
		#this app looks at the whole, upper and lower half and the 4 quaters of the page
		top = page.mediaBox.getUpperRight_y()
		right = page.mediaBox.getUpperRight_x()
		bottom = page.mediaBox.getLowerLeft_y()
		left = page.mediaBox.getLowerLeft_x()
		areas = [[top, left, bottom, right],
		         [top, left, (bottom+top)/2, right],[(bottom+top)/2, left, bottom, right],
		         [top, left, (bottom+top)/2, (right+left)/2],[top, (right+left)/2, (bottom+top)/2, right],
		         [(bottom+top)/2, (right+left)/2, bottom, right],[(bottom+top)/2, left, bottom, (right+left)/2]
		        ]

		k = re.compile(re_classes, re.IGNORECASE)
		j = re.compile(re_money, re.IGNORECASE)
		l = re.compile(re_percent, re.IGNORECASE)
		
		for page in pages_to_extract:
		    for area in areas:
		        series = pd.Series(index=HEADER)
		        series['name'] = self.title
		        try:
		            table = tabula.read_pdf(self.path, pages=page, area = area)
		            p = table.T.reset_index().T
		            p = p.apply(lambda x: x.astype(str).str.lower())
		            
		            #we traverse the table looking for valid asset classes and values
		            for row_ind, row in p.iterrows():
		                search = True
		                for col_ind, col in row.iteritems():
		                    #first we look for the asset class in the table
		                    if search:
		                    	#if there are several asset classes in a cell, we can't do anthing 
		                        if (len(k.findall(col))>1):
		                            break
		                        if bool(k.search(col)):
		                            asset = k.search(col).group(0)
		                            search = False
		                            #sometimes the value and the class ends up in the same cell
		                            if bool(j.search(col)):
		                                self.into_series(series, asset, j.search(col).group(0))
		                                break
		                            if bool(l.search(col)):
		                                self.into_series(series, asset, l.search(col).group(0), '%')
		                                break
		                    #I make the assumption that in the table structure, we'll first find the asset class and the value would follow
		                    else:
		                        if bool(j.search(col)):
		                            self.into_series(series, asset, j.search(col).group(0))
		                            break
		                        if bool(l.search(col)):
		                            self.into_series(series, asset, l.search(col).group(0), '%')
		                            break
		                        
		            if (series.count() > 3):
		                series['file'] = getcwd()+self.path+'#page='+str(page)
		                results.append(series)
		                if (series.count() > 5):
		                    break            
		        except Exception as inst:
		            print("Unsuccessful table search: page", page, inst)
		    print("Results after p", page, ": ", len(results))
		print(results)
		return results

	# If no tables are found, we continue trying to text mine the expected results. This Python2 script looks for conversational sentences.	
	def text_extract(self):
		print("####TEXT EXTRACTION####")
		
		tokenizer = nltk.tokenize.RegexpTokenizer(r''+re_percent+'|'+re_classes+'|'+re_money)
		regexp_tagger = nltk.tag.RegexpTagger(
		    [(r''+re_classes, 'CL'),
		     (r''+re_money, 'NU'),
		     (r''+re_percent, 'PC'),     
		    ])
		results = []
		
		for page in self.page_dict:
		    text = self.page_dict[page]
		    
		    #regex to find full sentences
		    h = re.compile('[A-Z][^\.][\s\S]*?[^A-Z][.?!](?![A-Z][^a-z]|\d)')
		    matches = h.findall(text)
		    
		    #dropping sentences that don't contain any keywords
		    to_drop = []
		    j = re.compile(re_classes+'|'+re_context, re.IGNORECASE)
		    k = re.compile(re_classes, re.IGNORECASE)
		    
		    #sentence is dropped if sentence has less than 2 contextual keywords and either doesn't have digits or asset class names
		    for i in range(len(matches)-1, -1, -1):
		        matches[i] = matches[i].replace('\n', ' ').replace('\r', '')
		        length = 0 if bool(j.search(matches[i])) else len(j.findall(matches[i]))
		        if(length < 2 and (not bool(k.search(matches[i])) or not bool(re.search(r'\d', matches[i])))):
		            to_drop.append(i)

		    for i in to_drop:
		        matches.pop(i)
		    
			#tokenize matches into the useful tokens and tag them whether they're nubmers or classes blabla
		    for match in matches:
		        series = pd.Series(index=HEADER)
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
                #different combinations of pc, num, cla
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
		                    self.into_series(series, cla, num)
		                    self.into_series(series, cla, per, '%')
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
		                   self.into_series(series, cla, per, '%')
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
		                    self.into_series(series, cla, num)
		                    num = cla = None
		        if (series.count() > 3):
		            series['file'] = getcwd()+self.path+'#page='+str(page)
		            series['name'] = self.title
		            results.append(series)
		            if (series.count() > 5):
		            	break            
		        
		    print("Results after p", page, ": ", len(results))       
		
		return results

	#managing putting items into pandas sereis
	def into_series(self, series, type, value, percent = ''):
		if (bool(re.search(r"equit(?:y|ies)|stocks?", type, re.IGNORECASE))):
			series['equity'+percent] = value
		elif (bool(re.search(r"fixed income|bonds?", type, re.IGNORECASE))):
			series['fixed income'+percent] = value
		elif (bool(re.search(r"multi[- ]asset|hybrid", type, re.IGNORECASE))):
			series['multi-asset'+percent] = value
		elif (bool(re.search(r"alternatives?", type, re.IGNORECASE))):
			series['alternative'+percent] = value
		elif (bool(re.search(r"cash management|money market", type, re.IGNORECASE))):
			series['money market'+percent] = value

	def evaluate(self, candidates):
		best = candidates[0]
		for i in candidates:
			if (i.count() > best.count()):
				best = i
		return best

	def write_csv(self, series, best):
		output = open('results.csv', 'a') if best else open('candidates.csv', 'a')
		writer = csv.writer(output, dialect='excel')
		for i in series:
			writer.writerow(i.tolist())
		output.close()

	def clean_up(self):
		files = [f for f in listdir('./temp') if isfile(join('./temp', f))]
		for file in files:
			DeleteFile('./temp/'+file)