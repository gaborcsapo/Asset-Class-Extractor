import subprocess
import re
import tabula
import nltk
from os import getcwd, listdir, getpid, remove
from os.path import isfile, join
import pandas as pd
from PyPDF2 import PdfFileReader
import csv
from multiprocessing import Pool 
from tqdm import tqdm

#regexs to distinguish dollar values, asset classes and percent values
re_digits = "\d+(?:[ ,\.-]?\d+){0,3}"
re_currency = "(?:USD|US dollars?|dollars?|[$¢£¤¥֏؋৲৳৻૱௹฿៛\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6])"
re_words = "(?:(?:million|thousand|hundred|billion|M|B|T|K|m|bn|tn)(?![a-z]))"

re_context = '(?:mix|management|assets|investment|managed|invested)?'
re_headings = '(?:Fund mix|Assets|Group investments|Investment allocation|Fund management|Investment management|assets|sset class|aum)'
re_classes = '(?:equities|equity|fixed income|alternatives?|multi[ -]assets?|hybrid|cash management|money markets?|bonds?|stocks?)'
re_money = re_currency+'?\s?'+re_digits+'\s?'+re_words+'?\s?'+re_currency+'?(?![%\d])'
re_percent = re_digits+'%'

HEADER = ['name', 'date', 'equity', 'equity%', 'fixed income', 'fixed income%', 'alternative', 'alternative%', 'multi-asset','multi-asset%', 'money market', 'money market%', 'file']

class Document:
	def __init__(self, file):
		self.path = "./docs/" + file
		self.title = None
		self.date = None
		self.OFFSET = 0
		self.pdf_reader = PdfFileReader(open(self.path, "rb"))
		self.page_dict = {}

def main():
	files_pdf = [f for f in listdir('./docs') if (isfile(join('./docs', f)) and f.endswith('.pdf'))]
	clean_up('.log')

	pool = Pool()
	for _ in tqdm(pool.imap_unordered(extract, files_pdf), total=len(files_pdf)):
		pass
	pool.close() 
	pool.join()
	
	candidates_out = open('candidates.csv', 'w')
	writer = csv.writer(candidates_out, dialect='excel')
	writer.writerow(HEADER)
	for file in [f for f in listdir('./temp') if (isfile(join('./temp', f)) and f.endswith('.csv') and f.startswith('candidates'))]:
		with open('./temp/'+file, 'r') as f:
			for line in f:
				candidates_out.write(line)
	
	results_out = open('results.csv', 'w')
	writer = csv.writer(results_out, dialect='excel')
	writer.writerow(HEADER)
	for file in [f for f in listdir('./temp') if (isfile(join('./temp', f)) and f.endswith('.csv') and f.startswith('results'))]:
		with open('./temp/'+file, 'r') as f:
			for line in f:
				results_out.write(line)

	results_out.close()
	candidates_out.close()
	clean_up('.csv')

def extract(doc_name):
	doc = Document(doc_name)

	#cleaning output files
	output = open('./temp/results'+str(getpid())+'.csv', 'w')
	output.close()
	output = open('./temp/candidates'+str(getpid())+'.csv', 'w')
	output.close()

	global log_file
	log_file = open('./temp/logging_pid:'+str(getpid())+'.log', 'a')
	
	check_title(doc, doc.path)
	load_doc_dummy(doc)
	page_filter(doc)
	
	candidates = []
	candidates += table_extract(doc, [doc.OFFSET]) #table_extract(doc, doc.page_dict) #
	candidates += text_extract(doc)
	if (len(candidates) == 0):
		print('No candidates found in', doc.title, file=log_file)
		return
	best = evaluate(candidates)
	
	write_csv([best], True)
	write_csv(candidates, False)
	clean_up('.txt')
	log_file.close()

def check_title(doc, file):
	#checking if name meets standards for later printing use, and 
	title = re.compile("\d{2,3}\. [\S\s]+? \([A-Za-z ]+\)")
	date = re.compile("\d{4} ?Q?[1-4]?")
	title_search = title.search(file[7:])
	date_search = date.search(file[7:])
	if (bool(title_search) and bool(date_search)):
	    doc.title = title_search.group(0)
	    doc.date = date_search.group(0)
	    print("####", doc.title, doc.date, "####", file=log_file)
	else:
		print(file, " doesn't meet naming conventions", file=log_file)
		doc.title = 'UNKOWN'
		doc.date = 'UNKOWN'
	doc.OFFSET = int(file[7:9])

def load_doc_dummy(doc):
	print("####LOADING PDF####", file=log_file)
	
	try:
		doc.pdf_reader.decrypt('')
	except:
		pass
	pages = 3 #doc.pdf_reader.getNumPages()
	
	# I load the pages into the document object.
	for page in range(pages):
	    doc.page_dict[doc.OFFSET+page] = ''
	    
	    #converting to text using python2 script, which output to ./temp
	    cmd = ["python2", "./Libraries/pdf2txt/tools/pdf2txt.py", "-o", "./temp/output"+str(doc.OFFSET+page)+".txt", "-p", str(doc.OFFSET+page),"-t", "text", doc.path]
	    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	    temp, error = p.communicate()
	    if error:
	        print(error.decode("utf-8"), file=log_file)
	    
	    #reading in files to get text from ./temp   
	    with open("./temp/output"+str(doc.OFFSET+page)+".txt", "r") as page_output:
	        for line in page_output:
	            doc.page_dict[doc.OFFSET+page] += line.strip() + ' '
	    page_output.close()

	print("Document loaded", file=log_file)

def load_doc(doc):
	print("####LOADING PDF####", file=log_file)
	
	try:
		doc.pdf_reader.decrypt('')
	except:
		pass
	pages = doc.pdf_reader.getNumPages()
	
	# I load the pages into the document object.
	for page in range(pages):
	    print("p",page, " is loading", file=log_file)
	    doc.page_dict[page] = ''
	    
	    #converting to text using python2 script, which output to ./temp
	    cmd = ["python2", "./Libraries/pdf2txt/tools/pdf2txt.py", "-o", "./temp/output"+str(page)+".txt", "-p", str(page),"-t", "text", doc.path]
	    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	    temp, error = p.communicate()
	    if error:
	        print(error.decode("utf-8"), file=log_file)
	    
	    #reading in files to get text from ./temp   
	    with open("./temp/output"+str(page)+".txt", "r") as page_output:
	        for line in page_output:
	            doc.page_dict[page] += line.strip() + ' '
	    page_output.close()

	print("Document loaded", file=log_file)

# Once loaded, the document has to go through a basic page scoring step. If the page doesn't contain Asset/Investment allocation or the two words equity and fixed income, then the page is thrown away.
def page_filter(doc):
	page_nomination = []
	cases = "(?:"+re_headings+"|"+re_classes+")[\s\S]*""(?:"+re_headings+"|"+re_classes+")"
	p = re.compile(cases, re.IGNORECASE)
	for page in doc.page_dict:
	    matches = p.findall(doc.page_dict[page])
	    if matches:
	        page_nomination.append(page)
	#taking subset of original document object
	doc.page_dict = {k: doc.page_dict[k] for k in page_nomination}
	print("Pages to look through:", len(doc.page_dict), file=log_file)

# To analyze the pages, first I'll try to convert it to a table and then I'll validate the first table column entries (equity, fixed income, and optionally multi asset and alternatives) I'll also try to look for columns indicating years.
def table_extract(doc, pages_to_extract):
	print("####TABLE EXTRACTION####", file=log_file)
	results = []
	try:
	    page = doc.pdf_reader.getPage(0)
	except:
	    doc.pdf_reader.decrypt('')
	    page = doc.pdf_reader.getPage(0)

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
	        series['name'] = doc.title
	        series['date'] = doc.date
	        try:
	            table = tabula.read_pdf(doc.path, pages=page, area = area, silent=True)
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
	                                into_series(series, asset, j.search(col).group(0))
	                                break
	                            if bool(l.search(col)):
	                                into_series(series, asset, l.search(col).group(0), '%')
	                                break
	                    #I make the assumption that in the table structure, we'll first find the asset class and the value would follow
	                    else:
	                        if bool(j.search(col)):
	                            into_series(series, asset, j.search(col).group(0))
	                            break
	                        if bool(l.search(col)):
	                            into_series(series, asset, l.search(col).group(0), '%')
	                            break
	                        
	            if (series.count() > 3):
	                series['file'] = getcwd()+doc.path+'#page='+str(page)
	                results.append(series)
	                print("Results += 1 on p", page, file=log_file)
	                if (series.count() > 5):
	                    break            
	        except Exception as inst:
	            print("Unsuccessful table search: page", page, inst, file=log_file)
	    
	return results

# If no tables are found, we continue trying to text mine the expected results. This Python2 script looks for conversational sentences.	
def text_extract(doc):
	print("####TEXT EXTRACTION####", file=log_file)
	
	tokenizer = nltk.tokenize.RegexpTokenizer(r''+re_percent+'|'+re_classes+'|'+re_money)
	regexp_tagger = nltk.tag.RegexpTagger(
	    [(r''+re_classes, 'CL'),
	     (r''+re_money, 'NU'),
	     (r''+re_percent, 'PC'),     
	    ])
	results = []
	
	for page in doc.page_dict:
	    text = doc.page_dict[page]
	    
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
	                        break
	                if i[1] == 'NU':
	                    if num == None:
	                        num = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                    if cla == None:
	                        cla = i[0]
	                    else:
	                        break
	                if per and num and cla:
	                    into_series(series, cla, num)
	                    into_series(series, cla, per, '%')
	                    per = num = cla = None
	                    
	        elif (cl == pc):
	            for i in tags:        
	                if i[1] == 'PC':
	                    if per == None:
	                        per = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                   if cla == None:
	                       cla = i[0]
	                   else:
	                       break
	                if per and cla:
	                   into_series(series, cla, per, '%')
	                   cla = per = None
	        elif (cl == nu):
	            for i in tags:        
	                if i[1] == 'NU':
	                    if num == None:
	                       num = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                    if cla == None:
	                        cla = i[0]
	                    else:
	                        break
	                if num and cla:
	                    into_series(series, cla, num)
	                    num = cla = None
	        if (series.count() > 3):
	            series['file'] = getcwd()+doc.path+'#page='+str(page)
	            series['name'] = doc.title
	            series['date'] = doc.date
	            results.append(series)
	            print("Results +=1 on p", page, file=log_file)
	            if (series.count() > 5):
	            	break                      
	
	return results

def get_unit(doc, page, value):
	number = 1
	#looking through the value
	if (bool(re.search(r"trillion|\stn", value, re.IGNORECASE))):
		number *= 1000000000000
	elif (bool(re.search(r"billion|\sbn", value, re.IGNORECASE))):
		number *= 1000000000
	elif (bool(re.search(r"million", value, re.IGNORECASE))):
		number *= 1000000
	#looking through the pages
	# elif (bool(re.search(r"", doc[page], re.IGNORECASE))):
	# 	number *= 1000000000000
	# elif (bool(re.search(r"", doc[page], re.IGNORECASE))):
	# 	number *= 1000000000
	# elif (bool(re.search(r"", doc[page], re.IGNORECASE))):
	# 	number *= 1000000
	#last resort is guessing based on the size of the number
	elif (1000 > float(re.sub(r"\D", "", value)) > 1):
		number *= 1000000000
	elif (1000 < float(re.sub(r"\D", "", value))):
		number *= 1000000

	result = float(re.sub(r"\D", "", value))*number
	#currency check
	currency = (re.search(r"[¢£¤¥֏؋৲৳৻૱௹฿៛\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6]", value, re.IGNORECASE))
	if (True):
		pass #if there are more foreign than dollar
	if (bool(currency)):
		result = currency + str(number)

	return result


#managing putting items into pandas sereis
def into_series(series, type, value, percent = ''):
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

def evaluate(candidates):
	best = candidates[0]
	for i in candidates:
		if (i.count() > best.count()):
			best = i
	return best

def write_csv(series, best):
	output = open('./temp/results'+str(getpid())+'.csv', 'a') if best else open('./temp/candidates'+str(getpid())+'.csv', 'a')
	writer = csv.writer(output, dialect='excel')
	for i in series:
		writer.writerow(i.tolist())
	output.close()

def clean_up(ext):
	for file in [f for f in listdir('./temp') if (isfile(join('./temp', f)) and f.endswith(ext))]:
		remove('./temp/'+file)

if __name__ == "__main__": main()